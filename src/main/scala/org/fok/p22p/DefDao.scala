package org.fok.p22p

import scala.beans.BeanProperty

import com.google.protobuf.Message

import lombok.extern.slf4j.Slf4j
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.commons.SessionModules
import onight.oapi.scala.traits.OLog
import onight.osgi.annotation.NActorProvider
import onight.tfw.ojpa.api.DomainDaoSupport
import onight.tfw.ojpa.api.annotations.StoreDAO
import onight.tfw.oparam.api.OParam
import onight.tfw.otransio.api.IPacketSender
import onight.tfw.otransio.api.PSender
import onight.tfw.ntrans.api.annotation.ActorRequire
import onight.tfw.ojpa.api.StoreServiceProvider
import onight.tfw.ntrans.api.ActorService
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.outils.conf.PropHelper
import onight.tfw.ojpa.api.IJPAClient

import org.fok.p22p.model.P22P.PModule;
import org.fok.core.dbapi.ODBSupport;
import org.fok.core.cryptoapi.ICryptoHandler;

abstract class PSMPZP[T <: Message] extends SessionModules[T] with PBUtils with OLog {
  override def getModule: String = PModule.PZP.name()
}

@NActorProvider
@Slf4j
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IJPAClient]))
class InstDaos extends PSMPZP[Message] with ActorService {

  @StoreDAO(target = "fok_db", daoClass = classOf[ODSP22p])
  @BeanProperty
  var odb: ODBSupport = null;

  @StoreDAO(target = "fok_db", daoClass = classOf[ODSViewStateStorage])
  @BeanProperty
  var viewstateDB: ODBSupport = null;

  @ActorRequire(name = "bc_crypto", scope = "global") //  @BeanProperty
  var enc: ICryptoHandler = null;

  def setOdb(daodb: DomainDaoSupport) {
    if (daodb != null && daodb.isInstanceOf[ODBSupport]) {
      odb = daodb.asInstanceOf[ODBSupport];
      Daos.odb = odb;
    } else {
      log.warn("cannot set odb ODBSupport from:" + daodb);
    }
  }
  def setViewstateDB(daodb: DomainDaoSupport) {
    if (daodb != null && daodb.isInstanceOf[ODBSupport]) {
      viewstateDB = daodb.asInstanceOf[ODBSupport];
      Daos.viewstateDB = viewstateDB;
    } else {
      log.warn("cannot set viewstateDB ODBSupport from:" + daodb);
    }
  }

  def setEnc(_enc: ICryptoHandler) = {
    enc = _enc;
    Daos.enc = _enc;
  }
  def getEnc(): ICryptoHandler = {
    enc;
  }

}

object Daos {

  val props: PropHelper = new PropHelper(null);

  var odb: ODBSupport = null

  var viewstateDB: ODBSupport = null

  var enc: ICryptoHandler = null;

  def isDbReady(): Boolean = {
    return odb != null && odb.getDaosupport.isInstanceOf[ODBSupport] &&
      odb.getDaosupport != null &&
      odb.getDaosupport.getDaosupport != null &&
      viewstateDB != null && viewstateDB.getDaosupport.isInstanceOf[ODBSupport] &&
      enc != null;
  }
}

