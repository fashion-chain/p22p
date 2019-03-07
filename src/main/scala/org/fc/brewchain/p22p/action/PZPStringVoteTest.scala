package org.fc.brewchain.p22p.action

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import lombok.extern.slf4j.Slf4j
import onight.oapi.scala.commons.LService
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.traits.OLog
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.FramePacket
import org.fc.brewchain.bcapi.exception.FBSException
import org.apache.commons.lang3.StringUtils
import java.util.HashSet
import onight.tfw.outils.serialize.UUIDGenerator
import scala.collection.JavaConversions._
import org.apache.commons.codec.binary.Base64
import org.fc.brewchain.p22p.pbgens.P22P.PSJoin
import org.fc.brewchain.p22p.pbgens.P22P.PRetJoin
import org.fc.brewchain.p22p.PSMPZP
import org.fc.brewchain.p22p.pbgens.P22P.PCommand
import java.net.URL
import org.fc.brewchain.p22p.pbgens.P22P.PMNodeInfo
import org.fc.brewchain.p22p.exception.NodeInfoDuplicated
import org.fc.brewchain.p22p.pbgens.P22P.PVBase
import onight.tfw.mservice.NodeHelper
import com.google.protobuf.Any
import org.fc.brewchain.p22p.pbgens.P22P.PBVoteNodeIdx
import org.fc.brewchain.p22p.pbgens.P22P.PVType
import org.fc.brewchain.p22p.Daos
import org.fc.brewchain.p22p.pbft.StateStorage
import org.fc.brewchain.p22p.pbgens.P22P.PBFTStage
import org.fc.brewchain.p22p.core.MessageSender
import org.brewchain.bcapi.utils.PacketIMHelper._
import org.slf4j.MDC
import org.fc.brewchain.p22p.utils.LogHelper
import org.fc.brewchain.p22p.utils.LogHelper
import org.fc.brewchain.p22p.node.Network
import org.fc.brewchain.p22p.node.Networks
import org.fc.brewchain.bcapi.BCPacket
import org.fc.brewchain.p22p.pbft.VoteQueue
import org.fc.brewchain.p22p.pbft.DMVotingNodeBits
import org.fc.brewchain.p22p.pbft.DMViewChange
import org.fc.brewchain.p22p.pbgens.P22P.PSTestMessage
import org.fc.brewchain.p22p.pbgens.P22P.PRetTestMessage
import org.fc.brewchain.p22p.pbgens.P22P.TestMessageType
import org.fc.brewchain.bcapi.crypto.BitMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ConcurrentHashMap
import onight.osgi.annotation.ScalaActorProvider

import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import onight.tfw.otransio.api.session.CMDService
import org.fc.brewchain.p22p.pbgens.P22P.PBVoteString
import org.fc.brewchain.p22p.pbgens.P22P.PBVoteString.PVStatus
import org.fc.brewchain.p22p.pbgens.P22P.PBVoteString.PVOperation
import org.brewchain.bcapi.gens.Oentity.OValue
import org.fc.brewchain.p22p.utils.Config
import com.google.protobuf.ByteString

@NActorProvider
@Slf4j
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PZPStringVoteTest extends PSMPZP[PBVoteString] {
  override def service = PZPStringVoteTestService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPStringVoteTestService extends OLog with PBUtils with LService[PBVoteString] with PMNodeHelper with LogHelper {

  override def onPBPacket(pack: FramePacket, pbo: PBVoteString, handler: CompleteHandler) = {
    val messageid =
      if (StringUtils.isBlank(pbo.getMessageid)) {
        UUIDGenerator.generate();
      } else {
        pbo.getMessageid
      }
    var ret = pbo.toBuilder()
    implicit val network = networkByID(pbo.getNid)
    if (network == null) {
      ret.setStatus(PVStatus.PVS_NOT_READY)
      ret.setVoteResult(pbo.getVoteContent + "_network_not_ready");
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        pbo.getOp match {
          case PVOperation.VO_VOTESTART =>
            val vbase = PVBase.newBuilder();
            vbase.setV(1)
            vbase.setN(1)
            vbase.setMType(PVType.STR_PBFT_VOTE)
            vbase.setMessageUid(UUIDGenerator.generate())
            vbase.setNid(network.netid)
            vbase.setOriginBcuid(network.root().bcuid)
            vbase.setFromBcuid(network.root.bcuid);
            vbase.setCreateTime(System.currentTimeMillis())
            vbase.setLastUpdateTime(vbase.getCreateTime)
            if(StringUtils.isNotBlank(pbo.getExtType))
            {
              vbase.setExtType(pbo.getExtType)
            }
            vbase.setContents(toByteString(PBVoteString.newBuilder()
              .setGcmd("ttspzp").setMessageid(messageid).setNid(pbo.getNid)
              .setVoteContent("" + System.currentTimeMillis() / 60000)));
            val ov = Daos.viewstateDB.get(network.stateStorage.STR_seq(vbase)).get match {
              case ov if ov == null =>
                OValue.newBuilder();
              case ov if ov != null =>
                val pbdb = PVBase.newBuilder().mergeFrom(ov.getExtdata)
                if (System.currentTimeMillis() - pbdb.getLastUpdateTime < Config.MIN_EPOCH_EACH_VOTE) {
                  null
                } else {
                  ov.toBuilder()
                }
              case _ =>
                null;
            }
            if (ov != null) {
              Daos.viewstateDB.put(network.stateStorage.STR_seq(vbase), ov
                .setExtdata(
                  ByteString.copyFrom(vbase.setState(PBFTStage.PRE_PREPARE).build().toByteArray())).clearSecondKey().build())
              network.voteQueue.appendInQ(vbase.setState(PBFTStage.PENDING_SEND).build())
              //        wallMessage(network,vbase.build())
            }
          case PVOperation.VO_MAKEDECISION =>
            ret.setStatus(PVStatus.PVS_VOTED)
            //一分钟都确定
            ret.setVoteResult("" + System.currentTimeMillis() / 60000);
          case PVOperation.VO_FINALMERGE =>
            log.debug("final mergeMessage:OK:" + pbo.getVoteContent);
          case _ =>
            log.debug("unknow operation:" + pbo.getOp);
        }
        //      }
      } catch {
        case e: FBSException => {
          ret.clear()
          ret.setVoteResult(pbo.getVoteContent + "_err_" + e.getMessage);
        }
        case t: Throwable => {
          log.error("error:", t);
          ret.clear()
          ret.setVoteResult(pbo.getVoteContent + "_err_" + t.getMessage)
        }
      } finally {
        try {
          handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
        } finally {
          //          MDCRemoveMessageID
        }
      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.TTS.name();
}