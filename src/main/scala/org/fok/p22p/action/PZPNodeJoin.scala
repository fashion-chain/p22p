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
import org.fok.p22p.node.Networks
import org.fok.p22p.node.PNode
import org.fok.p22p.utils.PacketIMHelper._
import org.fok.p22p.pbft.VoteWorker
import org.fok.p22p.utils.LogHelper
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.extender.internal.linker.ManagedType

import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import org.fok.p22p.core.MessageSender
import onight.tfw.outils.serialize.SessionIDGenerator

@NActorProvider
@Slf4j
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PZPNodeJoin extends PSMPZP[PSJoin] {
  override def service = PZPNodeJoinService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPNodeJoinService extends LogHelper with PBUtils with LService[PSJoin] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSJoin, handler: CompleteHandler) = {
    log.debug("JoinService::" + pack.getFrom() + ",OP=" + pbo.getOp)
    var ret = PRetJoin.newBuilder();
    val network = networkByID(pbo.getNid)
    if (network == null) {
      ret.setRetCode(-1).setRetMessage("unknow network:" + pbo.getNid)
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(network)
        //       pbo.getMyInfo.getNodeName
        val from = pbo.getMyInfo;
        //        log.debug("verify Message=="+MessageSender.verifyMessage(pack)(network));
        ret.setMyInfo(toPMNode(network.root))
        if (StringUtils.isBlank(from.getBcuid) || StringUtils.isBlank(from.getPubKey)
            || StringUtils.isBlank(from.getUri)) {
          log.debug("get empty bcuid");  
          ret.setRetCode(-1).setRetMessage("unknow id");
        } else if (!SessionIDGenerator.checkSum(from.getBcuid.substring(1))) {
          log.error("invalid bcuid:" + from.getBcuid);
          ret.setRetCode(-4).setRetMessage("bcuid invalid");
          MessageSender.dropNode(from.getBcuid)
        } else if (network.joinNetwork.blackList.containsKey(from.getBcuid)) {
          log.error("black current bcuid:" + from.getBcuid)
          ret.setRetCode(-5).setRetMessage("bcuid in black list:" + from.getBcuid);
          MessageSender.dropNode(from.getBcuid)
        } else if (pbo.getOp == PSJoin.Operation.NODE_CONNECT) {
          System.setProperty("java.protocol.handler.pkgs", "org.fc.brewchain.bcapi.url");
          log.debug("getURI:" + from.getUri)
          //          from.getUri.split(",").map { new URL(_) } //checking uri
          //          val _urlcheck = new URL(from.getUri)
          val samenode = StringUtils.equals(pbo.getNetworkInstance,
            Networks.instanceid);
          if (samenode &&
            ((from.getTryNodeIdx > 0 && from.getTryNodeIdx == network.root().node_idx) ||
              StringUtils.equals(from.getBcuid, network.root().bcuid))) {
            log.info("same NodeIdx :" + from.getNodeIdx + ",tryIdx=" + from.getTryNodeIdx + ",bcuid=" + from.getBcuid + ",netid=" +
              samenode + ":" + pbo.getNetworkInstance + "-->" + Networks.instanceid);
            //            MessageSender.dropNode(from.getBcuid);
            ret.setRetCode(-1)
            throw new NodeInfoDuplicated("NodeIdx=" + from.getNodeIdx);
          } else if (network.node_bits.testBit(from.getTryNodeIdx)) {
            network.nodeByIdx(from.getTryNodeIdx) match {
              case Some(n) if n.bcuid.endsWith(from.getBcuid) && !samenode && from.getTryNodeIdx != network.root().node_idx =>
                log.debug("node back online ")
                network.onlineMap.put(n.bcuid, n);
              case _ =>
                ret.setRetCode(-2)
                log.info("nodebits duplicated NodeIdx directnode:" + from.getNodeIdx);
                throw new NodeInfoDuplicated("NodeIdx=" + from.getNodeIdx);
            }
          } else {
            //name, idx, protocol, address, port, startup_time, pub_key, counter,idx
            val ccpending = network.pendingNodes.filter { n =>
              n.try_node_idx == from.getTryNodeIdx && ! from.getBcuid.equals(n.bcuid) && !StringUtils.equals(from.getPubKey, n.pub_key)
            }.size
            val ccdnodes = network.directNodes.filter { n =>
              n.node_idx == from.getTryNodeIdx && ! from.getBcuid.equals(n.bcuid) && !StringUtils.equals(from.getPubKey, n.pub_key)
            }.size
            if (ccdnodes+ccpending > 0) {
              ret.setRetCode(-2)
              log.error("nodebits duplicated NodeIdx pending :" + from.getNodeIdx+",ccp="+ccpending+",ccd="+ccdnodes);
              throw new NodeInfoDuplicated("NodeIdx=" + from.getNodeIdx);
            } else {
              val n = fromPMNode(from);
              log.error("add Pending Node:bcuid=" + n.bcuid+",tryidx="+from.getTryNodeIdx);
              network.onlineMap.put(n.bcuid, n);
              network.addPendingNode(n)
              val allNC = (network.directNodeByBcuid.size +
                network.pendingNodeByBcuid.size)
              if (pbo.getNodeCount >= allNC * 2 / 3
                && (pbo.getNodeNotifiedCount >= pbo.getNodeCount - 1)) {
                log.debug("start join network and vote")
                network.voteNodeMap.runOnce();
              } else {
                log.debug("cannot start join network and vote:NC=" + pbo.getNodeCount + ",all=" + allNC)
              }
            }
          }
        } else if (pbo.getOp == PSJoin.Operation.NODE_CONNECT) {
          //        NodeInstance.curnode.addPendingNode(new LinkNode(from.getProtocol, from.getNodeName, from.getAddress, // 
          //          from.getPort, from.getStartupTime, from.getPubKey, from.getTryNodeIdx, from.getNodeIdx))
        }

        //      ret.addNodes(toPMNode(NodeInstance.root));

        network.directNodes.map { _pn =>
          log.debug("direct.node==" + _pn.bcuid)
          ret.addNodes(toPMNode(_pn));
        }
        network.pendingNodes.map { _pn =>
          log.debug("pending.node==" + _pn.bcuid)
          ret.addNodes(toPMNode(_pn));
        }
      } catch {
        case fe: NodeInfoDuplicated => {
          ret.setMyInfo(toPMNode(network.root))
          ret.addNodes(toPMNode(network.root));
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
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.JIN.name();
}
