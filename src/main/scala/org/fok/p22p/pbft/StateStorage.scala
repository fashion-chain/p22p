package org.fok.p22p.pbft

import java.util.concurrent.atomic.AtomicInteger
import org.fok.p22p.Daos
import org.apache.commons.lang3.StringUtils
import onight.oapi.scala.traits.OLog
import org.fok.p22p.model.P22P.PVBase
import org.fok.p22p.model.P22P.PBFTStage
import scala.concurrent.impl.Future
import com.google.protobuf.ByteString
import onight.tfw.otransio.api.PacketHelper
import org.fok.p22p.model.P22P.PVType
import org.fok.p22p.model.P22P.PVBaseOrBuilder
import org.fok.tools.time.JodaTimeHelper
import java.util.ArrayList
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import org.fok.p22p.core.Votes
import org.fok.p22p.core.Votes.VoteResult
import org.fok.p22p.core.Votes.NotConverge
import org.fok.p22p.core.Votes.Converge
import org.fok.p22p.core.Votes.Undecisible
import onight.tfw.mservice.NodeHelper
import org.fok.p22p.utils.Config
import org.apache.commons.codec.binary.Base64
import org.fok.p22p.node.Network
import org.fok.tools.bytes.BytesHashMap

case class StateStorage(network: Network) extends OLog {
  def STR_seq(pbo: PVBaseOrBuilder): String = STR_seq(pbo.getMTypeValue, pbo.getExtType match {
    case null => ""
    case v: String => v
  })
  def STR_seq(uid: Int, extstr: String = ""): String = "v_seq_" + network.netid + "." + uid

  def nextV(pbo: PVBase.Builder): Int = {
    this.synchronized {
      val (retv, newstate) = Daos.viewstateDB.get(STR_seq(pbo).getBytes).get match {
        case ov if ov != null =>
          PVBase.newBuilder().mergeFrom(ov) match {
            case dbpbo if dbpbo.getState == PBFTStage.REPLY =>
              if (System.currentTimeMillis() - dbpbo.getLastUpdateTime < Config.MIN_EPOCH_EACH_VOTE) {
                log.debug("cannot start next vote: time less than past" + dbpbo.getState + ",V=" + dbpbo.getV + ",DIS=" + JodaTimeHelper.secondFromNow(dbpbo.getLastUpdateTime));
                (-1, PBFTStage.REJECT);
              } else {
                log.debug("getting next vote:" + dbpbo.getState + ",V=" + dbpbo.getV);
                pbo.setViewCounter(dbpbo.getViewCounter + 1)
                pbo.setStoreNum(dbpbo.getStoreNum)
                (dbpbo.getV + 1, PBFTStage.PRE_PREPARE)
              }
            case dbpbo if System.currentTimeMillis() - dbpbo.getLastUpdateTime < Config.MIN_EPOCH_EACH_VOTE && dbpbo.getState == PBFTStage.INIT || System.currentTimeMillis() - dbpbo.getCreateTime > Config.TIMEOUT_STATE_VIEW =>
              log.debug("recover from vote:" + dbpbo.getState + ",lastCreateTime:" + JodaTimeHelper.format(dbpbo.getCreateTime));
              pbo.setViewCounter(dbpbo.getViewCounter)
              pbo.setStoreNum(dbpbo.getStoreNum)
              getStageV(pbo.build()) match {
                case fv if fv == null =>
                  (dbpbo.getV, PBFTStage.PRE_PREPARE)
                case fv if fv != null =>
                  (dbpbo.getV + 1, PBFTStage.PRE_PREPARE)
              }
            case dbpbo =>
              log.debug("cannot start vote:" + dbpbo.getState + ",past=" + JodaTimeHelper.secondFromNow(dbpbo.getCreateTime) + ",O=" + dbpbo.getOriginBcuid);
              (-1, PBFTStage.REJECT);
          }
        case _ =>
          log.debug("New State ,db is empty");
          pbo.setViewCounter(1)
          pbo.setStoreNum(1)
          (1, PBFTStage.PRE_PREPARE);
      }
      if (Config.VOTE_DEBUG) return -1;
      if (retv > 0) {
        Daos.viewstateDB.put(STR_seq(pbo).getBytes,
          pbo.setV(retv)
              .setCreateTime(System.currentTimeMillis())
              .setLastUpdateTime(System.currentTimeMillis())
              .setFromBcuid(network.root().bcuid)
              .setOriginBcuid(network.root().bcuid)
              .setState(newstate)
              .build().toByteArray());
      }
      retv
    }
  }

  def mergeViewState(pbo: PVBase): Option[Array[Byte]] = {
    Daos.viewstateDB.get(STR_seq(pbo).getBytes).get match {
      case ov if ov != null && StringUtils.equals(pbo.getOriginBcuid, network.root().bcuid) =>
        Some(ov) // from locals
      case ov if ov != null =>
        PVBase.newBuilder().mergeFrom(ov) match {
          case dbpbo if StringUtils.equals(dbpbo.getOriginBcuid, pbo.getOriginBcuid) && pbo.getV == dbpbo.getV
            && dbpbo.getStateValue <= pbo.getStateValue =>
            Some(ov);
          case dbpbo if StringUtils.equals(dbpbo.getOriginBcuid, pbo.getOriginBcuid) && pbo.getV == dbpbo.getV
            && dbpbo.getStateValue > pbo.getStateValue =>
            log.debug("state low dbV=" + dbpbo.getStateValue + ",pbV=" + pbo.getStateValue + ",V=" + pbo.getV + ",f=" + pbo.getFromBcuid)
            Some(ov)
          case dbpbo if (System.currentTimeMillis() - dbpbo.getLastUpdateTime > Config.TIMEOUT_STATE_VIEW)
            && pbo.getV >= dbpbo.getV =>
            Some(ov);
          case dbpbo if pbo.getV >= dbpbo.getV && (
            dbpbo.getState == PBFTStage.INIT || dbpbo.getState == PBFTStage.REPLY
            || StringUtils.isBlank(dbpbo.getOriginBcuid)) =>
            Some(ov);
          case dbpbo if (dbpbo.getState == PBFTStage.REJECT || dbpbo.getRejectState == PBFTStage.REJECT)
            && !StringUtils.equals(dbpbo.getOriginBcuid, pbo.getOriginBcuid) =>
            Some(ov);
          case dbpbo @ _ =>
            pbo.getState match {
              case PBFTStage.COMMIT => //already know by other.
                log.debug("other nodes commited!")
                updateNodeStage(pbo, PBFTStage.COMMIT);
                voteNodeStages(pbo) match {
                  case n: Converge if n.decision == pbo.getState =>
                    log.debug("OtherVoteCommit::MergeOK,PS=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid + ",from=" + pbo.getFromBcuid);
                    Daos.viewstateDB.put(STR_seq(pbo).getBytes,
                      pbo.toBuilder()
                          .setFromBcuid(network.root().bcuid)
                          .setState(PBFTStage.INIT)
                          .setV(pbo.getV).setStoreNum(pbo.getStoreNum).setViewCounter(pbo.getViewCounter)
                          .build().toByteArray())
                    None;
                  case _ =>
                    log.debug("OtherVoteCommit::NotMerge,PS=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid + ",from=" + pbo.getFromBcuid);
                    None;
                }
              case _ =>
                log.debug("Cannot MergeView For local state Not EQUAL:"
                  + "db.[O=" + dbpbo.getOriginBcuid + ",F=" + dbpbo.getFromBcuid + ",V=" + dbpbo.getV + ",S=" + dbpbo.getStateValue
                  + ",RJ=" + dbpbo.getRejectState
                  + ",TO=" + JodaTimeHelper.secondFromNow(dbpbo.getLastUpdateTime)
                  + "],p[O=" + pbo.getOriginBcuid + ",F=" + pbo.getFromBcuid + ",V=" + pbo.getV + ",S=" + pbo.getStateValue
                  + ",RJ=" + pbo.getRejectState + "]");
                None;
            }

        }
      case _ =>
        val ov = pbo.toByteArray()
        Some(ov)
    }
  }

  def saveStageV(pbo: PVBase, ov: Array[Byte]) {
    val key = STR_seq(pbo) + ".F." + pbo.getV;
    Daos.viewstateDB.put(key.getBytes, ov);
  }

  def getStageV(pbo: PVBase) {
    val key = STR_seq(pbo) + ".F." + pbo.getV;
    Daos.viewstateDB.get(key.getBytes).get;
  }

  def updateTopViewState(pbo: PVBase) {
    this.synchronized({
      val ov = Daos.viewstateDB.get(STR_seq(pbo).getBytes).get
      if (ov != null) {
        Daos.viewstateDB.put(STR_seq(pbo).getBytes,
            pbo.toBuilder().setLastUpdateTime(System.currentTimeMillis()).build().toByteArray());
      }
    })
  }

  def saveIfNotExist(pbo: PVBase, ov: Array[Byte] , newstate: PBFTStage): PBFTStage = {
    val dbkey = STR_seq(pbo) + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV + "." + newstate;
    Daos.viewstateDB.get(dbkey.getBytes).get match {
      case ov if ov != null =>
        log.debug("Omit duplicated=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);
        PBFTStage.DUPLICATE;
      case _ =>
        Daos.viewstateDB.put(dbkey.getBytes, ov);
        newstate
    }
  }
  def makeVote(pbo: PVBase, ov: Array[Byte], newstate: PBFTStage)(implicit dm: Votable = null): PBFTStage = {
    val dbkey = STR_seq(pbo) + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV + ".";
    Daos.viewstateDB.get((dbkey + newstate).getBytes).get match {
      case ov if ov != null =>
        PBFTStage.DUPLICATE;
      case _ =>
        voteNodeStages(pbo) match {
          case n: Converge if n.decision == pbo.getState =>
            log.debug("Vote::MergeOK,PS=" + pbo.getState + ",New=" + newstate + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid + ",from=" + pbo.getFromBcuid);
            Daos.viewstateDB.put(STR_seq(pbo).getBytes, 
                pbo.toBuilder()
                .setState(newstate)
                .setLastUpdateTime(System.currentTimeMillis())
                .build().toByteArray())
            if (newstate != PBFTStage.PRE_PREPARE && newstate != PBFTStage.PREPARE) {
              saveIfNotExist(pbo, ov, newstate);
            }
            newstate

          case un: Undecisible =>
            log.debug("Vote::Undecisible:State=" + pbo.getState + ",desc=" + un + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);
            PBFTStage.NOOP
          case no: NotConverge =>
            log.debug("Vote::Not Converge:State=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);

            Daos.viewstateDB.put((dbkey + pbo.getState).getBytes, pbo.toBuilder()
                .setRejectState(PBFTStage.NOOP)
                .setLastUpdateTime(System.currentTimeMillis())
                .build().toByteArray());

            if (StringUtils.equals(pbo.getOriginBcuid, network.root().bcuid)) {
              log.debug("Reject for My Vote ")
              Daos.viewstateDB.put(STR_seq(pbo).getBytes,
                pbo.toBuilder()
                    .setFromBcuid(network.root().bcuid)
                    .setState(PBFTStage.INIT)
                    .setLastUpdateTime(System.currentTimeMillis() + Config.getRandSleepForBan())
                    .build().toByteArray())
            }
            PBFTStage.NOOP
          case n: Converge if n.decision == PBFTStage.REJECT =>
            if (Daos.viewstateDB.get((dbkey + pbo.getState).getBytes) != null) {
              log.debug("omit reject ConvergeState:" + n.decision + ",NewState=" + newstate + ",pbostate=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);
              PBFTStage.NOOP
            } else {
              Daos.viewstateDB.put((dbkey + pbo.getState).getBytes, pbo.toBuilder()
                .setRejectState(PBFTStage.REJECT)
                .setLastUpdateTime(System.currentTimeMillis())
                .build().toByteArray());
              log.warn("getRject ConvergeState:" + n.decision + ",NewState=" + newstate + ",pbostate=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);
              if (StringUtils.equals(pbo.getOriginBcuid, network.root().bcuid)) {
                log.debug("Reject for this Vote ")
                Daos.viewstateDB.put(STR_seq(pbo).getBytes,
                 pbo.toBuilder()
                      .setFromBcuid(network.root().bcuid)
                      .setState(PBFTStage.INIT)
                      .setLastUpdateTime(System.currentTimeMillis() + Config.getRandSleepForBan())
                      .build().toByteArray())
              } else {
                log.debug("Reject for other Vote ")
                Daos.viewstateDB.put(STR_seq(pbo).getBytes,
                 pbo.toBuilder()
                      .setFromBcuid(network.root().bcuid)
                      .setState(PBFTStage.REJECT).setRejectState(PBFTStage.REJECT)
                      .setLastUpdateTime(System.currentTimeMillis() + Config.getRandSleepForBan())
                      .build().toByteArray())
              }
              PBFTStage.REJECT
            }
          case n: Converge =>
            log.warn("unknow ConvergeState:" + n.decision + ",NewState=" + newstate + ",pbostate=" + pbo.getState);
            PBFTStage.NOOP
          case _ =>
            PBFTStage.NOOP
        }
      //        }
    }
  }
  def updateNodeStage(pbo: PVBase, state: PBFTStage): PBFTStage = {
    val strkey = STR_seq(pbo);
    val newpbo = if (state != pbo.getState) pbo.toBuilder().setState(state).setLastUpdateTime(System.currentTimeMillis()).build()
    else
      pbo;
    
    Daos.viewstateDB.put((strkey + "." + pbo.getFromBcuid + "." + pbo.getMessageUid + "." + pbo.getV + "." + newpbo.getStateValue).getBytes,(strkey + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV).getBytes, newpbo.toByteArray());
    state
  }
  def outputList(ovs: BytesHashMap[Array[Byte]]): Unit = {
    ovs.map { x =>
      val p = PVBase.newBuilder().mergeFrom(x._2);
      log.debug("-------::DBList:State=" + p.getState + ",V=" + p.getV + ",N=" + p.getN + ",SN=" + p.getStoreNum + ",VC=" + p.getViewCounter + ",O=" + p.getOriginBcuid
        + ",F=" + p.getFromBcuid + ",REJRECT=" + p.getRejectState + ",KEY=?")
    }
  }
  def voteNodeStages(pbo: PVBase)(implicit dm: Votable = null): VoteResult = {
    val strkey = STR_seq(pbo);
    val ovs = Daos.viewstateDB.listBySecondKey((strkey + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV).getBytes);
    if (ovs.get != null && ovs.get.size() > 0) {
      val reallist = ovs.get.filter { ov => PVBase.newBuilder().mergeFrom(ov._2).getStateValue == pbo.getStateValue };
      log.debug("get list:allsize=" + ovs.get.size() + ",statesize=" + reallist.size + ",state=" + pbo.getState)
      outputList(ovs.get)
      if (dm != null) { //Vote only pass half
        dm.voteList(network, pbo, reallist)
      } else {  
        Undecisible()
      }
    } else {
      Undecisible()
    }
  }
  val VIEW_ID_PROP = "org.bc.pbft.view.state"

}