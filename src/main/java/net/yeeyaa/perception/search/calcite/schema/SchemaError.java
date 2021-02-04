package net.yeeyaa.perception.search.calcite.schema;

import net.yeeyaa.eight.PlatformException.Type;


public enum SchemaError implements Type {
	ERROR_PARAMETERS,
	CREATE_TIMEOUT,
	SCHEMA_EXCEPTION;
	
	@Override
	public String getMessage() {	
		return name();
	}

	@Override
	public void setMessage(String message) {}

	@Override
	public Integer getCode() {
		return ordinal();
	}

	@Override
	public void setCode(Integer code) {}

	@Override
	public String getCate() {
		return getDeclaringClass().getSimpleName();
	}
	
	@Override
	public void setCate(String cate) {}
}
