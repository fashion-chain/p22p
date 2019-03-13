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
import org.fok.p22p.PSMPZP
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService

@NActorProvider
@Instantiate(name = "pzpctrl")
@Provides(specifications = Array(classOf[ActorService]), strategy = "SINGLETON")
class PZPCtrl extends PSMPZP[Message] with OLog {

  def networkByID(netid: String): Network = {
    Networks.networkByID(netid);
  }
  
  override def getCmds: Array[String] = Array("CTL");
  
}

