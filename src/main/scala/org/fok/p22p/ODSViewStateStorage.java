package org.fok.p22p;

import org.fok.core.dbapi.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;

public class ODSViewStateStorage extends ODBDao {

	public ODSViewStateStorage(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "pzp_viewstate.nodename";
	}
//	public static void main(String[] args) {
//		System.out.println("pzp_viewstate.nodename".split("\\.")[1]);
//	}

	
}
