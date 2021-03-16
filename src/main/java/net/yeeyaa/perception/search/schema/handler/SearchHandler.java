package net.yeeyaa.perception.search.schema.handler;

import java.nio.ByteOrder;
import java.sql.Types;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.cache.Cache;

import net.yeeyaa.eight.IBiProcessor;
import net.yeeyaa.eight.ITriProcessor;
import net.yeeyaa.perception.search.schema.Constants;
import net.yeeyaa.perception.search.schema.Node;
import net.yeeyaa.perception.search.schema.Node.Category;


public class SearchHandler implements ITriProcessor<Object, IBiProcessor<String, Object[], Object>, Map<String, Object>, ITriProcessor<Object, String, Object, Object>>{
    protected final Logger log;
    protected final Object serviceid;
    protected final ObjectMapper om = new ObjectMapper().enableDefaultTyping(ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT).setSerializationInclusion(Include.NON_NULL);
	protected final Map<String, Integer> tableMeta = new LinkedHashMap<String, Integer>();
	protected final Map<String, Integer> columnMeta = new LinkedHashMap<String, Integer>();
	public enum Fuction{schema, meta, table}
	
	{
		tableMeta.put(Constants.Name, Types.VARCHAR);
		tableMeta.put(Constants.Remark, Types.VARCHAR);
		tableMeta.put(Constants.Ref, Types.VARCHAR);
		columnMeta.put(Constants.Name, Types.VARCHAR);
		columnMeta.put(Constants.Remark, Types.VARCHAR);
		columnMeta.put(Constants.Ref, Types.VARCHAR);
		columnMeta.put(Constants.Type, Types.VARCHAR);
		columnMeta.put(Constants.Direct, Types.INTEGER);
		String charset = (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) ? "UTF-16BE" : "UTF-16LE";
		System.setProperty("saffron.default.charset", charset);
		System.setProperty("saffron.default.nationalcharset", charset);
		System.setProperty("saffron.default.collation.name", charset + "$en_US");
	}
	
	public SearchHandler() {
		this.log = LoggerFactory.getLogger(SearchHandler.class);
		this.serviceid = UUID.randomUUID();
	}
	
	public SearchHandler(Logger log) {
		this.log = log == null ? LoggerFactory.getLogger(SearchHandler.class) : log;
		this.serviceid = UUID.randomUUID();
	}

	public SearchHandler(Object serviceid) {
		this.log = LoggerFactory.getLogger(SearchHandler.class);
		this.serviceid = serviceid;
	}
	
	public SearchHandler(Logger log, Object serviceid) {
		this.log = log == null ? LoggerFactory.getLogger(SearchHandler.class) : log;
		this.serviceid = serviceid;
	}
	
	@Override
	public ITriProcessor<Object, String, Object, Object> operate(final Object name, final IBiProcessor<String, Object[], Object> holder, final Map<String, Object> parameter) {
		return new ITriProcessor<Object, String, Object, Object>() {
			protected final String id = (String) parameter.get("user");
			protected final String dataset = parameter.get("pwd").toString();
			
			@Override
			public Object operate(Object key, String fuction, Object params) {
				switch(Fuction.valueOf(fuction)) {
					case schema: return schema(key, holder);
					case meta: return meta(key, holder, params);
					case table: return table(key, holder, params);
				}
				return null;
			}

			protected List<Object> table(Object key, IBiProcessor<String, Object[], Object> holder, Object params) {
				Cache<Object, List<Object>> cache = (Cache<Object, List<Object>>) holder.perform("getCache", new Object[] {name, serviceid});
				List<Object> value = cache == null ? null : cache.getIfPresent(params == null ? "search.meta." + dataset + "." + id : "search.meta." + dataset + "." + id + "." + params);
				if (value == null) try {
					value = new LinkedList<Object>();
					List<Node[]> tables = (List<Node[]>) holder.perform("get", new Object[]{Category.DATASET.value() + "*" + dataset, Category.DATASET.value() + "*" + dataset + "*" + id});
					if (tables != null)	if (params == null) {
						value.add(tableMeta);
						for (Node[] ts : tables) for (Node t : ts) {
							Object[] m = new Object[3];
							m[0] = t.getName();
							Map<String, Object> config = t.getConfig();
							if (config != null) {
								if (config.get(Constants.Remark) != null) m[1] = config.get(Constants.Remark);
								String ref = (String) config.get(Constants.Ref);
								if (ref != null) {
									int index = ref.indexOf('*');
									m[2] = (index > -1 ? ref.substring(0, index) : ref) + "." + t.getName();
								}
							}
							value.add(m);
						}
						if (cache != null) cache.put("search.meta." + dataset + "." + id, value);
					} else {
						value.add(columnMeta);
						for (Node[] ts : tables) for (Node t : ts) if (params.equals(t.getName()) && t.getSub() != null) for (Node col : t.getSub()) {
							Object[] m = new Object[5];
							m[0] = col.getName();
							Map<String, Object> config = col.getConfig();
							if (config != null && config.get(Constants.Remark) != null) m[1] = config.get(Constants.Remark);
							Object ref = config == null || config.get(Constants.Ref) == null ? col.getName() : config.get(Constants.Ref);
							Object type = config == null || config.get(Constants.Type) == null ? ref : config.get(Constants.Type);
							m[2] = ref;
							m[3] = getType(type).name();
							m[4] = getType(type).getValue();
							value.add(m);
						}
						if (cache != null) cache.put("search.meta." + dataset + "." + id + "." + params, value);
					}
				} catch (Exception e) {
                	log.error("search meta fail.", e);
                } 
				return value;
			}
			
			protected List<Object> meta(Object key, IBiProcessor<String, Object[], Object> holder, Object params) {
				Cache<Object, List<Object>> cache = (Cache<Object, List<Object>>) holder.perform("getCache", new Object[] {name, serviceid});
				List<Object> value = cache == null ? null : cache.getIfPresent(params == null ? "search.meta." + dataset + "." + id : "search.meta." + dataset + "." + id + "." + params);
				if (value == null) try {
					value = new LinkedList<Object>();
					List<Node[]> tables = (List<Node[]>) holder.perform("get", new Object[]{Category.DATASET.value() + "*" + dataset, Category.DATASET.value() + "*" + dataset + "*" + id});
					if (tables != null)	if (params == null) {
						value.add(tableMeta);
						for (Node[] ts : tables) for (Node t : ts) {
							Object[] m = new Object[3];
							m[0] = t.getName();
							Map<String, Object> config = t.getConfig();
							if (config != null) {
								if (config.get(Constants.Remark) != null) m[1] = config.get(Constants.Remark);
								String ref = (String) config.get(Constants.Ref);
								if (ref != null) {
									int index = ref.indexOf('*');
									m[2] = (index > -1 ? ref.substring(0, index) : ref) + "." + t.getName();
								}
							}
							value.add(m);
						}
						if (cache != null) cache.put("search.meta." + dataset + "." + id, value);
					} else {
						value.add(columnMeta);
						for (Node[] ts : tables) for (Node t : ts) if (params.equals(t.getName()) && t.getSub() != null) for (Node col : t.getSub()) {
							Object[] m = new Object[5];
							m[0] = col.getName();
							Map<String, Object> config = col.getConfig();
							if (config != null && config.get(Constants.Remark) != null) m[1] = config.get(Constants.Remark);
							Object ref = config == null || config.get(Constants.Ref) == null ? col.getName() : config.get(Constants.Ref);
							Object type = config == null || config.get(Constants.Type) == null ? ref : config.get(Constants.Type);
							m[2] = ref;
							m[3] = getType(type).name();
							m[4] = getType(type).getValue();
							value.add(m);
						}
						if (cache != null) cache.put("search.meta." + dataset + "." + id + "." + params, value);
					}
				} catch (Exception e) {
                	log.error("search meta fail.", e);
                } 
				return value;
			}
			
			protected Object schema(Object id, IBiProcessor<String, Object[], Object> holder) {
				Cache<String, Object> cache = (Cache<String, Object>) holder.perform("getCache", new Object[] {name, serviceid});
				Object value = cache == null ? null : cache.asMap().get("search.schema." + dataset);
				if (value == null) try {
	            	List<Node[]> ss = (List<Node[]>) holder.perform("get", new Object[]{Constants.Root});
	                if (ss != null && ss.size() > 0)  {
                    	Map<String, Node> schemas = new HashMap<String, Node>();
                    	Node schema = null;
                    	HashSet<Node> has = new HashSet<Node>();
                    	for (Node s : ss.get(0)) switch (s.getCategory()) {
                    		case SCHEMA : schemas.put(s.getName(), s);
                    			break;
                    		case DATASET : if (s.getName().equals(dataset)) schema = s;
                    	}
                    	if (schema != null) {
                    		HashMap<String, Object> conf = new HashMap<String, Object>(schema.getConfig());
                        	for (Node v : schema.getSub()) if (v.getCategory().equals(Category.VIEW)) {
                        		Map<String, Object> config = v.getConfig();
                        		if (config != null && config.get(Constants.Ref) != null) {
                        			String s = config.get(Constants.Ref).toString().split("\\*")[0];
                        			if (schemas.containsKey(s)) has.add(schemas.get(s));
                        		}
                        	}
        					ObjectNode jsonObject = om.createObjectNode();
        	    			Properties clientinfo = new Properties();
        					Map<String, Entry<Map<String, Object>,Map<String, Object>>> add = new HashMap<String, Entry<Map<String, Object>,Map<String, Object>>>();
        	                if (conf != null) for (Entry<String, Object> entry : conf.entrySet()) {
        	                	int index = entry.getKey().indexOf('*');
        	                	switch (index) {
        	                		case -1 : jsonObject.put(entry.getKey(), entry.getValue().toString());
        	                			break;
        	                		case 0 : clientinfo.put(entry.getKey().substring(1), entry.getValue());
        	                			break;
        	                		default : String key = entry.getKey().substring(0, index);
        	                			Entry<Map<String, Object>,Map<String, Object>> map = add.get(key);
        			                	if (map == null) {
        			                		map = new SimpleImmutableEntry<Map<String, Object>,Map<String, Object>>(new HashMap<String, Object>(), new HashMap<String, Object>());
        			                		add.put(key, map);
        			                	}
        			                	if (entry.getKey().charAt(index + 1) == '*') map.getValue().put(entry.getKey().substring(index + 2), entry.getValue());
        			                	else map.getKey().put(entry.getKey().substring(index + 1), entry.getValue());
        	                	}
        		            }
        	                ArrayNode schemaJSONArray = om.createArrayNode();
        	                for(Node node : has) {
        	                	Map<String, Object> config = node.getConfig();
        	                    ObjectNode schemaNode = om.createObjectNode();
        	                    schemaNode.put(Constants.Name, node.getName());
        	                    schemaNode.put("factory", config.get(Constants.Type).toString());
        	                    schemaNode.put(Constants.Type, "custom");
        	                    ObjectNode operandNode = om.createObjectNode();
        	                    operandNode.put("dataset", dataset);
        	                    for (Entry<String, Object> entry : config.entrySet()) operandNode.put(entry.getKey(), entry.getValue().toString());
        	                    Entry<Map<String, Object>,Map<String, Object>> map = add.get(node.getName());
        	                    if (map != null) {
        	                    	for (Entry<String, Object> entry : map.getKey().entrySet()) schemaNode.put(entry.getKey(), entry.getValue().toString());
        	                    	for (Entry<String, Object> entry : map.getValue().entrySet()) operandNode.put(entry.getKey(), entry.getValue().toString());
        	                    }
        	                    schemaNode.set("operand", operandNode);
        	                    schemaJSONArray.add(schemaNode);
        	                }
        	                jsonObject.set("schemas", schemaJSONArray);
        	                clientinfo.setProperty("model", "inline:" + om.writeValueAsString(jsonObject));
        	                if (cache != null) cache.put("search.schema." + dataset, clientinfo);
        	                value = clientinfo;
                    	}
	        		}
				} catch (Exception e) {
                	log.error("search schema fail.", e);
                }
				return value;
			}
		};
	}
	
	public enum Type {
        BYTE(Types.TINYINT, "TINYINT"),
        SHORT(Types.SMALLINT, "SMALLINT"),
        INTEGER(Types.INTEGER, "INTEGER"),
        LONG(Types.BIGINT, "BIGINT"),
        FLOAT(Types.FLOAT, "FLOAT"),
        DOUBLE(Types.DOUBLE, "DOUBLE"),
        DECIMAL(Types.DECIMAL, "DECIMAL"),
        STRING(Types.VARCHAR, "VARCHAR"),
        BINARY(Types.BINARY, "BINARY"),
        BOOLEAN(Types.BOOLEAN, "BOOLEAN"),
        TIMESTAMP(Types.TIMESTAMP, "TIMESTAMP"),
        DATE(Types.DATE, "DATE"),
        TIME(Types.TIME, "TIME"),
        ARRAY(Types.ARRAY, "ARRAY"),
        OTHER(Types.OTHER, "OTHER"),
        DICT(Types.OTHER, "OTHER"),
        BITMAP(Types.BINARY, "BINARY"),
        ANY(Types.JAVA_OBJECT, "ANY");

        protected final Integer value;
        protected final String type;

        private Type(Integer value, String type) {
            this.value = value;
            this.type = type;
        }
        
        public Integer getValue() {
        	return value;
        }

		public String getType() {
			return type;
		}
    }
	
    public static Type getType(Object value) {
    	try {
    		return Type.valueOf(value.toString());
    	} catch (Exception e) {
    		return Type.STRING;
    	}
	}
}
