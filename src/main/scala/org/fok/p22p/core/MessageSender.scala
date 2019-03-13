package org.fok.p22p.core

import onight.tfw.otransio.api.PSender
import onight.tfw.otransio.api.IPacketSender
import scala.beans.BeanProperty
import onight.osgi.annotation.NActorProvider
import com.google.protobuf.Message
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.async.CallBack
import onight.tfw.ntrans.api.NActor
import onight.oapi.scala.traits.OLog
import org.fok.p22p.node.PNode
import onight.tfw.otransio.api.PackHeader
import org.fok.p22p.utils.BCPacket
import org.apache.commons.lang3.StringUtils
import org.fok.p22p.node.Networks
import com.google.protobuf.MessageOrBuilder

import org.fok.p22p.utils.PacketIMHelper._
import scala.collection.TraversableLike
import onight.tfw.otransio.api.NonePackSender
import org.fok.p22p.node.Network
import com.google.protobuf.ByteString
import org.fok.p22p.node.Node
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.otransio.api.PSenderService
import onight.tfw.ntrans.api.ActorService
import org.fok.p22p.utils.Config
import onight.tfw.ntrans.api.annotation.ActorRequire
import org.fok.core.cryptoapi.ICryptoHandler

@NActorProvider
@Provides(specifications = Array(classOf[ActorService], classOf[PSenderService]))
class CMessageSender extends NActor {

  //http. socket . or.  mq  are ok
  @PSender
  var sockSender: IPacketSender = new NonePackSender();

  def setSockSender(send: IPacketSender): Unit = {
    sockSender = send;
    MessageSender.sockSender = sockSender;
  }
  def getSockSender(): IPacketSender = {
    sockSender
  }
  @ActorRequire(name = "bc_crypto", scope = "global")
  var encApi: ICryptoHandler = null;
  def setEncApi(ecapi: ICryptoHandler): Unit = {
    encApi = ecapi;
    MessageSender.encApi = ecapi;
  }
  def getEncApi(): ICryptoHandler = {
    encApi
  }
}

object MessageSender extends OLog {
  var sockSender: IPacketSender = new NonePackSender();

  val PACK_SIGN = "_s";
  var encApi: ICryptoHandler = null;

  def appendUid(pack: BCPacket, node: Node)(implicit network: Network): Unit = {
    if (network.isLocalNode(node)) {
      pack.getExtHead.remove(PackHeader.PACK_TO);
    } else {
      pack.putHeader(PackHeader.PACK_TO, node.bcuid);
      pack.putHeader(PackHeader.PACK_URI, node.uri);
    }
    if (!pack.isBodySigned()) {
      val bb = pack.genBodyBytes()
      val shabb = encApi.sha256(bb);
      val signm = encApi.sign(encApi.hexStrToBytes(network.root().pri_key), shabb)
      pack.putHeader(PACK_SIGN,encApi.bytesToHexStr(signm));
      pack.setBodySigned(true);
    }
    pack.putHeader(PackHeader.PACK_FROM, network.root().bcuid);
  }

  def verifyMessage(pack: FramePacket): Option[Boolean] = {
    val fromuid = pack.getExtStrProp(PackHeader.PACK_FROM)
    if (StringUtils.isNotBlank(fromuid)) {
      val net = if (fromuid.startsWith("D")) {
        Networks.networkByID("dpos")
      } else if (fromuid.startsWith("R")) {
        Networks.networkByID("raft")
      } else {
        null
      }
      if (net != null) {
        val node = net.nodeByBcuid(fromuid)
        pack.getExtStrProp(PACK_SIGN) match {
          case n if StringUtils.isNotBlank(n) && n.length() >= 128 =>
            val pubkey = n.substring(0, 128);
            if (node == net.noneNode || StringUtils.equals(pubkey, node.pub_key)) {
              val bb = pack.getBody;
              val shabb = encApi.sha256(bb);
              val result = encApi.verify(encApi.hexStrToBytes(pubkey), shabb, encApi.hexStrToBytes(n));
              if (!result) {
                log.debug(fromuid + "messageverify error:" + pack.getExtHead + ",sign=" +
                  pack.getExtStrProp(MessageSender.PACK_SIGN) + ",sha=" + encApi.bytesToHexStr(shabb))
              }
              Some(result);
            } else {
              log.debug(fromuid + "messageverify error:fatal:" + pack.getExtHead + ",sign=" +
                pack.getExtStrProp(MessageSender.PACK_SIGN) + ",pubkey not equal:" +
                pubkey + ",nodepubkey=" + node.pub_key)
              Some(false);
            }
          case _ =>
            None
        }
      } else {
        None
      }
    } else {
      None
    }

  }

  def sendMessage(gcmd: String, body: Message, node: Node, cb: CallBack[FramePacket], priority: Byte = 0,timeoutms:Long = Config.TIMEOUT_MS_MESSAGE)(implicit network: Network) {
    val pack = BCPacket.buildSyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    if (priority > 0) {
      pack.getFixHead.setPrio(priority)
    }

    appendUid(pack, node)
    log.trace("sendMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
    try {
      cb.onSuccess(sockSender.send(pack, timeoutms))
    } catch {
      case e: Exception =>
        log.warn("sendMessageFailed:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo(), e)
        cb.onFailed(e, pack);
    }

  }

  def asendMessage(gcmd: String, body: Message, node: Node, cb: CallBack[FramePacket], priority: Byte = 0)(implicit network: Network) {
    val pack = BCPacket.buildSyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    if (priority > 0) {
      pack.getFixHead.setPrio(priority)
    }

    appendUid(pack, node)
//    log.trace("sendMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
    sockSender.asyncSend(pack, cb)
  }

//  def wallMessageToPending(gcmd: String, body: Message, priority: Byte = 0)(implicit network: Network) {
//    val pack = BCPacket.buildAsyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
//    if (priority > 0) {
//      pack.getFixHead.setPrio(priority)
//    }
//    log.trace("wallMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
//    network.pendingNodes.map { node =>
//      appendUid(pack, node)
//      sockSender.post(pack)
//    }
//  }

  def wallMessageToPending(gcmd: String, body: ByteString, priority: Byte = 0)(implicit network: Network) {
    val pack = BCPacket.buildAsyncFrom(body.toByteArray(), gcmd.substring(0, 3), gcmd.substring(3));
    if (priority > 0) {
      pack.getFixHead.setPrio(priority)
    }
    log.trace("wallMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
    network.pendingNodes.map { node =>
      appendUid(pack, node)
      sockSender.post(pack)
    }
  }

  def postMessage(gcmd: String, body: Either[Message, ByteString], node: Node, priority: Byte = 0)(implicit network: Network): Unit = {
    //    if("TTTPZP".equals(gcmd)){
    //      return;
    //    }
    val pack = body match {
      case Left(m) => BCPacket.buildAsyncFrom(m, gcmd.substring(0, 3), gcmd.substring(3));
      case Right(b) => BCPacket.buildAsyncFrom(b.toByteArray(), gcmd.substring(0, 3), gcmd.substring(3));
    }
    if (priority > 0) {
      pack.getFixHead.setPrio(priority)
    }
    appendUid(pack, node)
    //    log.trace("postMessage:" + pack)
    //    log.trace("postMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
    sockSender.post(pack)
  }

  def replyPostMessage(gcmd: String, node: Node, body: Message)(implicit network: Network) {
    val pack = BCPacket.buildAsyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    appendUid(pack, node); //frompack.getExtStrProp(PackHeader.PACK_FROM));
    sockSender.post(pack)
  }

  def dropNode(node: Node) {
    sockSender.tryDropConnection(node.bcuid);
  }
  def dropNode(bcuid: String) {
    sockSender.tryDropConnection(bcuid);
  }

  def changeNodeName(oldName: String, newName: String) {
    sockSender.changeNodeName(oldName, newName);
  }

  def setDestURI(bcuid: String, uri: String) {
    sockSender.setDestURI(bcuid, uri);
  }
}


