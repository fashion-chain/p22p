package org.fok.p22p.action

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
import org.fok.p22p.exception.FBSException
import org.apache.commons.lang3.StringUtils
import java.util.HashSet
import onight.tfw.outils.serialize.UUIDGenerator
import scala.collection.JavaConversions._
import org.apache.commons.codec.binary.Base64
import org.fok.p22p.model.P22P.PSJoin
import org.fok.p22p.model.P22P.PRetJoin
import org.fok.p22p.PSMPZP
import org.fok.p22p.model.P22P.PCommand
import java.net.URL
import org.fok.p22p.model.P22P.PMNodeInfo
import org.fok.p22p.exception.NodeInfoDuplicated
import org.fok.p22p.model.P22P.PVBase
import onight.tfw.mservice.NodeHelper
import com.google.protobuf.Any
import org.fok.p22p.model.P22P.PBVoteNodeIdx
import org.fok.p22p.model.P22P.PVType
import org.fok.p22p.Daos
import org.fok.p22p.pbft.StateStorage
import org.fok.p22p.model.P22P.PBFTStage
import org.fok.p22p.core.MessageSender
import org.fok.p22p.utils.PacketIMHelper._
import org.slf4j.MDC
import org.fok.p22p.utils.LogHelper
import org.fok.p22p.utils.LogHelper
import org.fok.p22p.node.Network
import org.fok.p22p.node.Networks
import org.fok.p22p.utils.BCPacket
import org.fok.p22p.pbft.VoteQueue
import org.fok.p22p.pbft.DMVotingNodeBits
import org.fok.p22p.pbft.DMViewChange
import org.fok.p22p.model.P22P.PSTestMessage
import org.fok.p22p.model.P22P.PRetTestMessage
import org.fok.p22p.model.P22P.TestMessageType
import org.fok.core.crypto.BitMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ConcurrentHashMap
import onight.osgi.annotation.ScalaActorProvider

import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import onight.tfw.otransio.api.session.CMDService
import org.fok.p22p.model.P22P.PSChangeNodeName

@NActorProvider
@Slf4j
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PZPChangeNodeName extends PSMPZP[PSChangeNodeName] {
  override def service = PZPChangeNodeNameService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPChangeNodeNameService extends OLog with PBUtils with LService[PSChangeNodeName] with PMNodeHelper with LogHelper {

  val cdlMap = new ConcurrentHashMap[String, CountDownLatch]; //new CountDownLatch(0)

  override def onPBPacket(pack: FramePacket, pbo: PSChangeNodeName, handler: CompleteHandler) = {
    var ret = PRetTestMessage.newBuilder();
    implicit val network = networkByID(pbo.getNid)
    if (network == null) {
      ret.setRetCode(-1).setRetMessage("unknow network:" + pbo.getNid)
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        if (StringUtils.isNotBlank(pbo.getNewname)) {
          ret.setRetCode(0);
          network.changeRootName(pbo.getNewname);
          network.pendingNodes.map { _pn =>
            log.debug("pending==" + _pn)
            if (StringUtils.equals(_pn.bcuid, network.root().bcuid)) {
              network.changePendingNode(_pn.changeName(network.root().name));
            }
          }
          network.directNodes.map { _pn =>
            log.debug("directnodes==" + _pn)
            if (StringUtils.equals(_pn.bcuid, network.root().bcuid)) {
              network.changeDirectNode(_pn.changeName(network.root().name));
            }
          }

        } else {
          ret.setRetCode(-1).setRetMessage("name cannot be blank");
        }
        //      }
      } catch {
        case fe: NodeInfoDuplicated => {
          ret.clear();
          ret.setRetCode(-1).setRetMessage("" + fe.getMessage)
        }
        case e: FBSException => {
          ret.clear()
          ret.setRetCode(-2).setRetMessage("" + e.getMessage)
        }
        case t: Throwable => {
          log.error("error:", t);
          ret.clear()
          ret.setRetCode(-3).setRetMessage("" + t.getMessage)
        }
      } finally {
        try {
          handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
        } finally {
        }
      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.CHN.name();
}