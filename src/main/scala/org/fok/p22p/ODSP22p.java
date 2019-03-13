package org.fok.p22p;

import org.fok.core.dbapi.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;

public class ODSP22p extends ODBDao {

	public ODSP22p(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "pzp";
	}

	
}
