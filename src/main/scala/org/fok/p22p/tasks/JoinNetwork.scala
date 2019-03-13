package org.fok.p22p.tasks

import java.util.concurrent.TimeUnit
import onight.oapi.scala.traits.OLog
import org.fok.p22p.model.P22P.PMNodeInfo
import org.fok.p22p.model.P22P.PBVoteNodeIdx
import java.math.BigInteger
import scala.collection.JavaConverters
import org.fok.p22p.node.PNode
import org.apache.commons.lang3.StringUtils
import org.fok.p22p.core.MessageSender
import org.fok.p22p.model.P22P.PSJoin
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CallBack
import org.fok.p22p.model.P22P.PRetJoin
import org.fok.p22p.Daos
import java.net.URL
import org.osgi.service.url.URLStreamHandlerService
import onight.tfw.otransio.api.PacketHelper
import scala.collection.mutable.ArrayBuffer
import java.util.HashMap
import org.fok.p22p.core.Votes._
import scala.collection.mutable.Map
import org.fok.p22p.action.PMNodeHelper
import org.fok.p22p.node.Networks
import scala.collection.JavaConversions._
import java.util.concurrent.ConcurrentHashMap
import org.fok.p22p.node.Network
import sun.rmi.log.LogHandler
import org.fok.p22p.utils.LogHelper
import java.util.concurrent.CountDownLatch
import org.fok.p22p.utils.SRunner
import com.google.protobuf.ByteString
import org.fok.p22p.model.P22P.PMJoinedNode

//投票决定当前的节点
case class JoinNetwork(network: Network, statupNodes: Iterable[PNode]) extends SRunner with PMNodeHelper with LogHelper {
  def getName() = "JoinNetwork"
  val sameNodes = new HashMap[Integer, PNode]();
  val pendingJoinNodes = new ConcurrentHashMap[String, PNode]();
  val joinedNodes = new HashMap[Integer, PNode]();
  val duplictedInfoNodes = Map[Int, PNode]();

  val startupDBLoadNodes = List[PMNodeInfo]();
  val blackList = new HashMap[String, String]();

  val DB_KEY = ("PZP__JoinNetwork__" + network.netid).getBytes

  def syncDB() {
    val pmb = PMJoinedNode.newBuilder()
    blackList.map(kvs =>
      pmb.addBlockBcuids(kvs._1 + "==>" + kvs._2))
    joinedNodes.map(f =>
      pmb.addJoinedNodes(toPMNode(f._2)))
    Daos.odb.put(DB_KEY,
      pmb.build().toByteArray())
  }
  def init() {
    loadFromDB()
  }
  def loadFromDB() {
    val v = Daos.odb.get(DB_KEY)
    if (v == null || v.get() == null) {
      log.debug("none block list or joined nodes");
    } else {
      val pmb = PMJoinedNode.newBuilder().mergeFrom(v.get)
      log.debug("load joined nodes info:" +
        pmb.getJoinedNodesList.foldLeft("")((a, b) => a + "," + b.getBcuid));
      log.debug("load block nodes info:" +
        pmb.getBlockBcuidsList.foldLeft("")((a, b) => a + "," + b));
      pmb.getBlockBcuidsList.map { x =>
        val kvs = x.split("==>");
        if (kvs.length == 2) {
          blackList.put(kvs(0).trim(), kvs(1).trim())
        }
      }
      pmb.getJoinedNodesList.map { pmnode =>
        startupDBLoadNodes.add(pmnode)
      }

    }
  }
  def runOnce() = {
    Thread.currentThread().setName("JoinNetwork");

    implicit val _net = network

    if (network.inNetwork() && pendingJoinNodes.size() <= 0
      && statupNodes.filter { x => joinedNodes.containsKey(x.uri.hashCode()) }.size == statupNodes.size) {
      log.debug("CurrentNode In Network:startupNodes.size=" + statupNodes.size + ",joinedNodeSize=" + joinedNodes.size());
      syncDB();
    } else {
      var hasNewNode = true;
      var joinLoopCount = 0;
      while (hasNewNode && joinLoopCount < statupNodes.size) {
        try {
          val failedNodes = new HashMap[String, PNode]();
          hasNewNode = false;
          joinLoopCount = joinLoopCount + 1;
          MDCSetBCUID(network);
          val namedNodes = (statupNodes ++ pendingJoinNodes.values()
            ++ startupDBLoadNodes.map { x => fromPMNode(x) }).filter { x =>
              StringUtils.isNotBlank(x.uri) && !sameNodes.containsKey(x.uri.hashCode()) && !joinedNodes.containsKey(x.uri.hashCode()) && //
                !network.isLocalNode(x) && !blackList.containsKey(x.bcuid())
            };

          if (startupDBLoadNodes.size > 0) {
            startupDBLoadNodes.clear();
          }
          val cdl = new CountDownLatch(namedNodes.size);
          namedNodes.map { n => //for each know Nodes
            //          val n = namedNodes(0);
            log.debug("JoinNetwork :Run----Try to Join :MainNet=" + n.uri + ",M.bcuid=" + n.bcuid() + ",cur=" + network.root.uri);
            if (!network.root.equals(n)) {
              val joinbody = PSJoin.newBuilder().setOp(PSJoin.Operation.NODE_CONNECT).setMyInfo(toPMNode(network.root()))
                .setNid(network.netid)
                .setNetworkInstance(Networks.instanceid)
                .setNodeCount(network.pendingNodeByBcuid.size
                  + network.directNodeByBcuid.size)
                .setNodeNotifiedCount(joinedNodes.size());
              val starttime = System.currentTimeMillis();

              log.debug("JoinNetwork :Start to Connect---:" + n.uri);

              MessageSender.asendMessage("JINPZP", joinbody.build(), n, new CallBack[FramePacket] {
                def onSuccess(fp: FramePacket) = {
                  MDCSetBCUID(network);
                  cdl.countDown()
                  namedNodes.synchronized({
                    if (fp.getBody == null || fp.getBody.length <= 10) {
                      log.error("send JINPZP warn:not ready " + n.uri + ",cost=" + (System.currentTimeMillis() - starttime))
                    } else {
                      log.info("send JINPZP success:to " + n.uri + ",cost=" + (System.currentTimeMillis() - starttime))
                      val retjoin = PRetJoin.newBuilder().mergeFrom(fp.getBody);
                      if (retjoin.getRetCode() == -1) { //same message
                        log.error("get Same Node:" + n.getName + ",n.uri=" + n.uri);
                        sameNodes.put(n.uri.hashCode(), n);
                        val newN = fromPMNode(retjoin.getMyInfo)
                        MessageSender.changeNodeName(n.bcuid, newN.bcuid);
                        MessageSender.dropNode(n)
                        network.onlineMap.put(newN.bcuid(), newN)
                        network.addPendingNode(newN);
                      } else if (retjoin.getRetCode() == -2) {
                        log.error("get duplex NodeIndex:" + n.getName);
                        sameNodes.put(n.uri.hashCode(), n);
                        duplictedInfoNodes.+=(n.uri.hashCode() -> n);
                      } else if (retjoin.getRetCode() == 0) {
                        joinedNodes.put(n.uri.hashCode(), n);
                        val newN = fromPMNode(retjoin.getMyInfo)
                        MessageSender.changeNodeName(n.bcuid, newN.bcuid);
                        network.addPendingNode(newN);
                        network.onlineMap.put(newN.bcuid(), newN)
                        retjoin.getNodesList.map { node =>
                          val pnode = fromPMNode(node);
                          if (network.addPendingNode(pnode)) {
                            pendingJoinNodes.put(node.getBcuid, pnode);
                          }
                          //
                        }
                      }
                      log.debug("get nodes:count=" + retjoin.getNodesCount + "," + sameNodes);
                    }
                  })
                }
                def onFailed(e: java.lang.Exception, fp: FramePacket) {
                  failedNodes.put(n.uri, n);
                  log.debug("send JINPZP ERROR " + n.uri + ",e=" + e.getMessage, e)
                  cdl.countDown()
                }
              });
            } else {
              cdl.countDown()
              log.debug("JoinNetwork :Finished ---- Current node is MainNode");
            }
          }
          try {
            cdl.await(60, TimeUnit.SECONDS);
          } catch {
            case t: Throwable =>
              log.debug("error connect to all nodes:" + t.getMessage, t);
          } finally {

          }
          log.debug("finished connect to all nodes");
          if (duplictedInfoNodes.size > 0 && !network.directNodeByBcuid.contains(network.root().bcuid) ) {
            //            val nl = duplictedInfoNodes.values.toSeq.PBFTVote { x => Some(x.node_idx) }
            //            nl.decision match {
            //              case Some(v: BigInteger) =>
            log.error("duplictedInfoNodes :" + duplictedInfoNodes.size + ",nameNode=" + namedNodes.size + ",pending=" +
              network.pendingNodes.size + ",dnode=" +
              network.directNodes.size + ",contains=" + network.directNodeByBcuid.contains(network.root().bcuid));
            if ((duplictedInfoNodes.size > network.pendingNodes.size / 3
              || duplictedInfoNodes.size > network.directNodes.size / 3) && !network.directNodeByBcuid.contains(network.root().bcuid)) {
              //            val nl = duplictedInfoNodes.values.toSeq.PBFTVote { x => Some(x.node_idx) }
              //            nl.decision match {
              //              case Some(v: BigInteger) =>
              log.error("duplictedInfoNodes ,change My Index:" + duplictedInfoNodes.size);
              network.removePendingNode(network.root())
              hasNewNode = true;
              joinLoopCount = 0;
              network.changeNodeIdx(duplictedInfoNodes.head._2.node_idx);
              //drop all connection first
              pendingJoinNodes.clear()
              joinedNodes.clear();
              sameNodes.clear();

              //              case _ => {
              //                log.debug("cannot get Converage :" + nl);
              //network.changeNodeIdx();
              //              }
              //            }
              //} else {
              //network.changeNodeIdx();
            }
            //          joinedNodes.clear();
            duplictedInfoNodes.clear();
          }
          if (namedNodes.size == 0) {
            log.debug("cannot reach more nodes. try from begining :namedNodes.size=" + namedNodes.size() + ",startupNodes.size=" + statupNodes.size + ",joinedSize=" + joinedNodes);
            statupNodes.map { x => joinedNodes.remove(x.uri().hashCode()) }
            //next run try another index;
          } else {
            if (pendingJoinNodes.size() > 0) {
              if (pendingJoinNodes.filter(p => !failedNodes.containsKey(p._2.uri())).size > 0) {
                hasNewNode = true;
              }
            }
          }
        } catch {
          case e: Throwable =>
            log.debug("JoinNetwork :Error", e);
        } finally {
          log.debug("JoinNetwork :[END]")
        }
      }
    }
  }
}