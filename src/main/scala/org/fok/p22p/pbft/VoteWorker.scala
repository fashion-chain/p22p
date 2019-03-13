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
import org.fok.p22p.utils.LogHelper
import org.fok.p22p.core.MessageSender
import org.fok.p22p.node.Networks
import onight.tfw.outils.serialize.UUIDGenerator
import org.fok.p22p.model.P22P.PBVoteViewChange
import org.fok.p22p.node.Network
import java.util.concurrent.LinkedBlockingQueue
import org.fok.p22p.utils.SRunner
import org.fok.p22p.action.PMNodeHelper

case class VoteWorker(network: Network, voteQueue: VoteQueue) extends SRunner with LogHelper with PMNodeHelper {

  def getName() = "VoteWorker"

  def wallMessage(network: Network, pbo: PVBase) = {
    
    if (pbo.getRejectState == PBFTStage.REJECT) {
      network.wallOutsideMessage("VOTPZP", Left(pbo), pbo.getMessageUid,'8')
    } else {
      network.wallMessage("VOTPZP", Left(pbo), pbo.getMessageUid,'8')
    }
  }

  def voteViewChange(network: Network, pbo1: PVBase) = {
    log.info("try start voteViewChange");
    if (StringUtils.equals(pbo1.getOriginBcuid, network.root().bcuid)) {
      val vbase = PVBase.newBuilder();
      vbase.setV(pbo1.getV + 1)
      vbase.setN(pbo1.getN)
      vbase.setMType(PVType.VIEW_CHANGE).setStoreNum(pbo1.getStoreNum + 1).setViewCounter(0)
      vbase.setMessageUid(UUIDGenerator.generate())
      vbase.setNid(network.netid)
      vbase.setOriginBcuid(network.root().bcuid)
      vbase.setFromBcuid(network.root.bcuid);
      vbase.setCreateTime(System.currentTimeMillis())
      vbase.setLastUpdateTime(vbase.getCreateTime)
      vbase.setContents(toByteString(PBVoteViewChange.newBuilder().setStoreNum(pbo1.getStoreNum + 1)
        .setViewCounter(0).setV(vbase.getV)));
      val ov = Daos.viewstateDB.get(network.stateStorage.STR_seq(vbase).getBytes).get match {
        case ov if ov == null =>
          null
        case ov if ov != null =>
          val pbdb = PVBase.newBuilder().mergeFrom(ov)
          if (System.currentTimeMillis() - pbdb.getLastUpdateTime < Config.MIN_EPOCH_EACH_VOTE) {
            null
          } else {
            ov
          }
        case _ =>
          null;
      }
      if (ov != null) {
        
        Daos.viewstateDB.put(network.stateStorage.STR_seq(vbase).getBytes, vbase.setState(PBFTStage.PRE_PREPARE).build().toByteArray())
        voteQueue.appendInQ(vbase.setState(PBFTStage.PENDING_SEND).build())
        //        wallMessage(network,vbase.build())
      }
    }
  }
  def makeVote(pbo: PVBase, ov: Array[Byte], newstate: PBFTStage) = {
    MDCSetMessageID(pbo.getMTypeValue + "|" + pbo.getMessageUid)
    val reply = pbo.toBuilder().setState(newstate)
      .setFromBcuid(network.root().bcuid)
      .setOldState(pbo.getState);
    if (pbo.getState == PBFTStage.PENDING_SEND) {
      log.debug("PendingSend=" + pbo.getState + ",trystate=" + newstate + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",O=" + pbo.getOriginBcuid);
      wallMessage(network, reply.build());
    } else {
      implicit val dm = pbo.getMType match {
        case PVType.NETWORK_IDX =>
          DMVotingNodeBits
        case PVType.VIEW_CHANGE =>
          DMViewChange
        case PVType.STR_PBFT_VOTE =>
          ExtStringVoter
        case _ => null;
      }
      log.debug("makeVote:State=" + pbo.getState + ",trystate=" + newstate + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",O=" + pbo.getOriginBcuid);

      newstate match {
        case PBFTStage.PRE_PREPARE =>
          log.debug("Vote::Move TO Next=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",=O=" + pbo.getOriginBcuid);

          wallMessage(network, reply.build());
        //        PBFTStage.PREPARE
        case PBFTStage.REJECT =>
          reply.setState(pbo.getState).setRejectState(PBFTStage.REJECT)
          //        log.info("MergeSuccess.Local!:V=" + pbo.getV + ",N=" + pbo.getN + ",org=" + pbo.getOriginBcuid)
          if (network.isLocalNode(pbo.getOriginBcuid)) {
            log.debug("omit reject Message for local:" + pbo.getFromBcuid);
            //          } else if (pbo.getRejectState == PBFTStage.REJECT) {
            //            log.debug("omit reject Message for remote:" + pbo.getFromBcuid);
          } else {
            //            MessageSender.replyPostMessage("VOTPZP", pbo.getFromBcuid, reply.build());
            val dbkey = network.stateStorage.STR_seq(pbo) + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV + "." + pbo.getState;
            Daos.viewstateDB.get(dbkey.getBytes).get match {
              case ov if ov != null => //&& PVBase.newBuilder().mergeFrom(ov.getExtdata).getState == newstate =>
                log.debug("Omit duplicated=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);
                PBFTStage.DUPLICATE;
              case _ =>
                Daos.viewstateDB.put(dbkey.getBytes, pbo.toBuilder()
                    .setFromBcuid(network.root().bcuid)
                    .setRejectState(PBFTStage.REJECT) .build().toByteArray())
                    //.setCount(pbo.getN)
                    //.setState(pbo.getState)
                    //.setV(pbo.getV)
                    //.setStoreNum(pbo.getStoreNum)
                    //.setViewCounter(pbo.getViewCounter)
                   
                wallMessage(network, reply.build());
            }
          }
        //          log.debug("OMit Rejct Message")
        //      case PBFTStage.REPLY =>
        //        network.stateStorage.saveStageV(pbo, ov.build());
        //        log.info("MergeSuccess.Local!:V=" + pbo.getV + ",N=" + pbo.getN + ",org=" + pbo.getOriginBcuid)
        case _ =>
          network.stateStorage.makeVote(pbo, ov, newstate) match {
            case PBFTStage.PREPARE | PBFTStage.COMMIT => //|| s == PBFTStage.REPLY =>
              log.debug("Vote::Move TO Next,State=" + reply.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",O=" + pbo.getOriginBcuid);
              wallMessage(network, reply.build())
            case PBFTStage.REJECT =>
              log.debug("Vote::Reject =" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",O=" + pbo.getOriginBcuid);
              reply.setState(pbo.getState).setRejectState(PBFTStage.REJECT)
              if (network.isLocalNode(pbo.getOriginBcuid)) {
                log.debug("omit reject Message afterVote for local:" + pbo.getOriginBcuid);
                //              } else if (pbo.getRejectState == PBFTStage.REJECT) {
                //                log.debug("omit reject Message for remote:" + pbo.getOriginBcuid);
              } else {
                wallMessage(network, reply.build());
                //                MessageSender.replyPostMessage("VOTPZP", pbo.getFromBcuid, reply.build());
              }
            case PBFTStage.REPLY =>
              network.stateStorage.saveStageV(pbo, ov);
              log.info("MergeSuccess." + pbo.getMType + ":V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",OF=" + pbo.getOriginBcuid)
              if (dm != null) {
                dm.finalConverge(network, pbo);
              }

              if (pbo.getViewCounter >= Config.NUM_VIEWS_EACH_SNAPSHOT && pbo.getMType != PVType.VIEW_CHANGE) {
                voteViewChange(network, pbo);
              }

            case PBFTStage.DUPLICATE =>
            //              log.info("Duplicated Vote Message!:V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",State=" + pbo.getState + ",org=" + pbo.getOriginBcuid)
            case s @ _ =>
            //              log.debug("Noop for state:" + newstate + ",voteresult=" + s)

          }
      }
    }
  }
  def runOnce() = {
    try {
      MDCSetBCUID(network);
      //      var (pbo, ov, newstate) = VoteQueue.pollQ();
      var hasWork = true;
      log.debug("Get Q size=" + voteQueue.inQ.size())
      while (hasWork) {
        val q = voteQueue.pollQ();
        if (q == null) {
          hasWork = false;
        } else {
          makeVote(q._1, q._2, q._3)
        }
      }
    } catch {
      case e: Throwable =>
        log.warn("unknow Error:" + e.getMessage, e)
    } finally {
      MDCRemoveMessageID()

    }
  }
}