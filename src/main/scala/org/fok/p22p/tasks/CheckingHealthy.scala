package org.fok.p22p.tasks

import java.util.concurrent.TimeUnit
import onight.oapi.scala.traits.OLog
import org.fok.p22p.model.P22P.PMNodeInfo
import org.fok.p22p.model.P22P.PBVoteNodeIdx
import java.math.BigInteger
import org.fok.p22p.node.PNode
import org.apache.commons.lang3.StringUtils
import org.fok.p22p.core.MessageSender
import org.fok.p22p.model.P22P.PSJoin
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CallBack
import org.fok.p22p.model.P22P.PRetJoin
import java.net.URL
import org.apache.felix.framework.URLHandlers
import org.fok.p22p.action.PMNodeHelper
import org.fok.core.crypto.BitMap
import org.fok.p22p.model.P22P.PVBase
import org.fok.p22p.model.P22P.PBFTStage
import org.fok.p22p.node.Networks
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import org.fok.p22p.model.P22P.PSNodeInfo
import org.fok.p22p.model.P22P.PRetNodeInfo

import scala.collection.JavaConversions._
import org.fok.p22p.node.Network
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.Map
import java.util.concurrent.atomic.AtomicInteger
import org.fok.p22p.utils.Config
import java.util.concurrent.CountDownLatch
import org.fok.p22p.utils.SRunner
import onight.tfw.outils.serialize.SessionIDGenerator
import org.fok.p22p.utils.LogHelper

//投票决定当前的节点
case class CheckingHealthy(network: Network) extends SRunner with PMNodeHelper with LogHelper {
  def getName() = "CheckingHealthy"
  val checking = new AtomicBoolean(false)
  val failedChecking = Map[String, AtomicInteger]();
  def runOnce() = {
    if (checking.compareAndSet(false, true)) {
      try {
        MDCSetBCUID(network)

        //    if (!StringUtils.isBlank(network.root().pub_key)) {
        val pack = PSNodeInfo.newBuilder().setNode(toPMNode(network.root()))
          .setNid(network.netid).build()
        implicit val _net = network
        val pn = network.pendingNodes.filter { _.bcuid != network.root().bcuid }
        val dn = network.directNodes.filter { _.bcuid != network.root().bcuid }
        val cdl = new CountDownLatch(pn.size + dn.size);
        pn.map { n =>
          try {
            if (StringUtils.isBlank(n.bcuid) || n.bcuid.length() <= 2 || !SessionIDGenerator.checkSum(n.bcuid.substring(1)) || StringUtils.isBlank(n.uri)
              || StringUtils.isBlank(n.pub_key)) {
              cdl.countDown();
              log.error("DropNode From Pending ERROR BCUID CheckSum:bcuid=" + ",n=" + n.bcuid + ",uri=" + n.uri + ";");
              network.removePendingNode(n);
              network.joinNetwork.joinedNodes.remove(n.uri.hashCode());
              network.joinNetwork.pendingJoinNodes.remove(n.bcuid)
              MessageSender.dropNode(n.bcuid)
            } else {
              log.debug("checking Health to pending@" + n.bcuid + ",uri=" + n.uri)
              MessageSender.asendMessage("HBTPZP", pack, n, new CallBack[FramePacket] {
                def onSuccess(fp: FramePacket) = {
                  cdl.countDown()
                  MDCSetBCUID(network)

                  log.debug("send HBTPZP success:to uri=" + n.uri + ",bcuid=" + n.bcuid)
                  failedChecking.remove(n.bcuid + "," + n.startup_time)
                  val retpack = PRetNodeInfo.newBuilder().mergeFrom(fp.getBody);

                  //          log.debug("get nodes:" + retpack);
                  if (retpack.getRetCode() == 0 && retpack.hasCurrent()) {
                    if (retpack.getCurrent == null) {
                      log.debug("Node EROR NotFOUND:" + retpack);
                      network.removePendingNode(n);
                    } else if (!StringUtils.equals(retpack.getCurrent.getBcuid, n.bcuid)) {
                      log.error("Node EROR BCUID Not Equal:" + retpack.getCurrent.getBcuid + ",n=" + n.bcuid + ",pack=" + retpack);
                      network.removePendingNode(n);
                      network.joinNetwork.joinedNodes.remove(n.uri.hashCode());
                      network.joinNetwork.pendingJoinNodes.remove(n.bcuid)
                      MessageSender.dropNode(n.bcuid)
                    } else {
                      log.debug("get nodes:pendingcount=" + retpack.getPnodesCount + ",dnodecount=" + retpack.getDnodesCount);
                      network.onlineMap.put(n.bcuid, n);
                      def joinFunc(pn: PMNodeInfo) = {
                        val pnode = fromPMNode(pn);
                        network.addPendingNode(pnode);
                        network.joinNetwork.pendingJoinNodes.put(pnode.bcuid, pnode)
                      }
                      retpack.getPnodesList.map(joinFunc)
                      retpack.getDnodesList.map(joinFunc)
                    }
                  } else {
                    log.error("hbt get empty pn=" + n + ",pack:" + retpack);
                  }
                }
                def onFailed(e: java.lang.Exception, fp: FramePacket) {
                  cdl.countDown()
                  MDCSetBCUID(network)

                  log.error("send HBTPZP ERROR uri=" + n.uri + ",bcuid=" + n.bcuid + ",startup=" + n.startup_time + ",e=" + e.getMessage, e)
                  failedChecking.get(n.bcuid + "," + n.startup_time) match {
                    case Some(cc) =>
                      if (cc.incrementAndGet() >= Config.HB_FAILED_COUNT) {
                        log.error("Drop Node for HeatBeat Failed!");
                        network.removePendingNode(n);
                        MessageSender.dropNode(n)
                        network.joinNetwork.joinedNodes.remove(n.uri.hashCode());
                        network.joinNetwork.pendingJoinNodes.remove(n.bcuid);
                      } else {
                        log.error("PNode warning. HeatBeat Feiled!:failedcc=" + cc.get, e);
                      }

                    case None =>
                      failedChecking.put(n.bcuid + "," + n.startup_time, new AtomicInteger(1));
                  }
                }
              }, '9');
            }
          } catch {
            case t: Throwable =>
              log.error("checking peningnodes error:"+n, t);
          }

        }

        dn.map { n =>
          try {
            if (!SessionIDGenerator.checkSum(n.bcuid.substring(1))) {
              cdl.countDown();
              log.debug("DropNode From DNodes ERROR BCUID CheckSum:bcuid=" + ",n=" + n.bcuid + ",uri=" + n.uri + ";");
              MessageSender.dropNode(n)
              network.joinNetwork.joinedNodes.remove(n.uri.hashCode());
              network.joinNetwork.pendingJoinNodes.remove(n.bcuid);
              network.removeDNode(n);
              network.removePendingNode(n);
            } else {
              log.debug("checking Health to directs@" + n.bcuid + ",uri=" + n.uri)
              val start = System.currentTimeMillis();
              MessageSender.asendMessage("HBTPZP", pack, n, new CallBack[FramePacket] {
                def onSuccess(fp: FramePacket) = {
                  cdl.countDown()
                  MDCSetBCUID(network)

                  log.debug("send HBTPZP Direct success:to " + n.uri + ",bcuid=" + n.bcuid)
                  network.onlineMap.put(n.bcuid, n);
                  failedChecking.remove(n.bcuid + "," + n.startup_time)
                  val retpack = PRetNodeInfo.newBuilder().mergeFrom(fp.getBody);
                  log.debug("get nodes:pendingcount=" + retpack.getPnodesCount + ",dnodecount=" + retpack.getDnodesCount);
                  if (retpack.getRetCode() == 0 && retpack.hasCurrent()) {
                    log.debug("get successful hbtpzp, dn=" + n);
                    if (retpack.getCurrent == null) {
                      log.debug("Node EROR NotFOUND:" + retpack);
                      network.joinNetwork.pendingJoinNodes.remove(n.bcuid);
                      network.removeDNode(n);
                    } else if (!StringUtils.equals(retpack.getCurrent.getBcuid, n.bcuid)) {
                      log.error("Node EROR BCUID Not Equal:" + retpack.getCurrent.getBcuid + ",n=" + n.bcuid);
                      network.removeDNode(n);
                      network.joinNetwork.joinedNodes.remove(n.uri.hashCode());
                      network.joinNetwork.pendingJoinNodes.remove(n.bcuid)
                      MessageSender.dropNode(n.bcuid)

                    } else {
                      retpack.getPnodesList.map { pn =>
                        network.addPendingNode(fromPMNode(pn));
                      }
                      //fix bugs when some node down.2018.3
                      retpack.getDnodesList.map { pn =>
                        if (network.nodeByBcuid(pn.getBcuid) == network.noneNode) {
                          network.addPendingNode(fromPMNode(pn));
                        }
                      }
                    }
                  } else {
                    log.error("hbt get empty dn=" + n + ",pack:" + retpack);

                  }
                }
                def onFailed(e: java.lang.Exception, fp: FramePacket) {
                  cdl.countDown()
                  MDCSetBCUID(network)

                  log.error("send HBTPZP Direct ERROR " + n.uri + ",startup=" + n.startup_time + ",cost=" + (System.currentTimeMillis() - start) + ",e=" + e.getMessage, e)
                  failedChecking.get(n.bcuid + "," + n.startup_time) match {
                    case Some(cc) =>
                      if (cc.incrementAndGet() >= Config.HB_FAILED_COUNT) {
                        log.error("Drop DNode for HeatBeat Failed!cc=" + cc);
                        MessageSender.dropNode(n)
                        network.joinNetwork.joinedNodes.remove(n.uri.hashCode());
                        network.joinNetwork.pendingJoinNodes.remove(n.bcuid);
                        network.removeDNode(n);
                        network.removePendingNode(n);
                      } else {
                        log.error("DNode warning. HeatBeat Feiled!:failedcc=" + cc.get + ",uri=" + n.uri, e);
                      }
                    case None =>
                      failedChecking.put(n.bcuid + "," + n.startup_time, new AtomicInteger(1));
                  }
                }

              }, '9');

              try {
                cdl.await(Math.min(Config.TICK_CHECK_HEALTHY, 10), TimeUnit.SECONDS)
              } catch {
                case t: Throwable =>
                  log.debug("checking Health wait error:" + t.getMessage, t);
              }
            }
          } catch {
            case t: Throwable =>
              log.error("checking dnodes error:"+n, t);
          }
        }
      } finally {
        checking.compareAndSet(true, false);
      }
    }
    //    }
  }
  //Scheduler.scheduleWithFixedDelay(new Runnable, initialDelay, delay, unit)
  def main(args: Array[String]): Unit = {
    System.setProperty("java.protocol.handler.pkgs", "org.fc.brewchain.bcapi.url");
    //System.setProperty("java.protocol.handler.pkgs", "org.fc.brewchain.bcapi.url");
    println(new URL("tcp://127.0.0.1:5100").getHost);
  }
}