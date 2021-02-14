package net.yeeyaa.perception.search.calcite.schema.handler;

import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.cache.Cache;

import net.yeeyaa.eight.IBiProcessor;
import net.yeeyaa.eight.ITriProcessor;
import net.yeeyaa.perception.search.calcite.schema.Constants;
import net.yeeyaa.perception.search.calcite.schema.Node;
import net.yeeyaa.perception.search.calcite.schema.Node.Category;


public class SearchHandler implements ITriProcessor<Object, IBiProcessor<String, Object[], Object>, Map<String, Object>, ITriProcessor<Object, String, Object[], Object>>{
    protected final Logger log;
    protected final Object serviceid;
    protected final ObjectMapper om = new ObjectMapper().enableDefaultTyping(ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT);
	protected final Map<String, String> tableMeta = new ConcurrentHashMap<String, String>();
	protected final Map<String, String> columnMeta = new ConcurrentHashMap<String, String>();
	public enum Fuction{schema, meta, type}
	{
		tableMeta.put(Constants.Name, Type.STRING.value());
		tableMeta.put(Constants.Remark, Type.STRING.value());
		tableMeta.put(Constants.Ref, Type.STRING.value());
		columnMeta.put(Constants.Name, Type.STRING.value());
		columnMeta.put(Constants.Remark, Type.STRING.value());
		columnMeta.put(Constants.Flag, Type.STRING.value());
		columnMeta.put(Constants.Type, Type.STRING.value());
		columnMeta.put(Constants.Ref, Type.STRING.value());
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
	public ITriProcessor<Object, String, Object[], Object> operate(final Object name, final IBiProcessor<String, Object[], Object> holder, final Map<String, Object> parameter) {
		return new ITriProcessor<Object, String, Object[], Object>() {
			protected final String id = (String) parameter.get("user");
			protected final String dataset = parameter.get("password").toString();
			
			@Override
			public Object operate(Object key, String fuction, Object[] params) {
				switch(Fuction.valueOf(fuction)) {
					case schema: return schema(key, holder, params);
					case type: return type(key, holder, params);
					case meta: return meta(key, holder, params);
				}
				return null;
			}
			
			protected Map<String, String> type(Object key, IBiProcessor<String, Object[], Object> holder, Object[] params) {
				return params == null || params.length == 0 || params[0] == null ? tableMeta : columnMeta;
			}

			protected List<Map<String, Object>> meta(Object key, IBiProcessor<String, Object[], Object> holder, Object[] params) {
				Cache<Object, List<Map<String, Object>>> cache = (Cache<Object, List<Map<String, Object>>>) holder.perform("getCache", new Object[] {name, serviceid});
				List<Map<String, Object>> value = cache.getIfPresent(params == null || params.length == 0 || params[0] == null ? "search.meta." + dataset + "." + id : "search.meta." + dataset + "." + id + "." + params[0]);
				if (value == null) try {
					value = new LinkedList<Map<String, Object>>();
					List<Node[]> tables = (List<Node[]>) holder.perform("get", new Object[]{Category.DATASET.value() + "*" + dataset, Category.DATASET.value() + "*" + dataset + "*" + id});
					if (params == null || params.length == 0 || params[0] == null) {
						if (tables != null) {
							for (Node[] ts : tables) for (Node t : ts) {
								Map<String, Object> map = new ConcurrentHashMap<String, Object>();
								map.put(Constants.Name, t.getName());
								Map<String, Object> config = t.getConfig();
								if (config != null) {
									if (config.get(Constants.Remark) != null) map.put(Constants.Remark, config.get(Constants.Remark));
									if (config.get(Constants.Ref) != null) if (config.get(Constants.Ref).toString().indexOf('*') > -1) map.put(Constants.Ref, config.get(Constants.Ref));
									else map.put(Constants.Ref, config.get(Constants.Ref) + "*" + t.getName());
								}
								value.add(map);
							}
							cache.put("search.meta." + dataset + "." + id, value);
						}
					} else if (tables != null) {
						for (Node[] ts : tables) for (Node t : ts) if (params[0].equals(t.getName()) && t.getSub() != null) for (Node col : t.getSub()) {
							Map<String, Object> map = new ConcurrentHashMap<String, Object>();
							map.put(Constants.Name, col.getName());
							Map<String, Object> config = t.getConfig();
							if (config != null && config.get(Constants.Remark) != null) map.put(Constants.Remark, config.get(Constants.Remark));
							Object ref = config == null || config.get(Constants.Ref) == null ? col.getName() : config.get(Constants.Ref);
							Object type = config == null || config.get(Constants.Type) == null ? ref : config.get(Constants.Type);
							map.put(Constants.Type, getType(type.toString()).value());
							value.add(map);
						}
						cache.put("search.meta." + dataset + "." + id + "." + params[0], value);
					}
				} catch (Exception e) {
                	log.error("search meta fail.", e);
                } 
				return value;
			}
			
			protected HashMap<String, Object> schema(Object key, IBiProcessor<String, Object[], Object> holder, Object[] params) {
				Cache<String, Object> cache = (Cache<String, Object>) holder.perform("getCache", new Object[] {name, serviceid});
				Object value = cache.asMap().get("search.schema." + dataset);
				if (value == null) try {
	            	List<Node[]> ss = (List<Node[]>) holder.perform("get", new Object[]{Constants.Root, Category.DATASET.value() + "*" + dataset});
	                if (ss != null && ss.size() > 1)  {
	                	Map<String, Node> schemas = new HashMap<String, Node>();
	                	for (Node s : ss.get(0)) if (s.getCategory().equals(Category.SCHEMA)) schemas.put(s.getName(), s);
	                	HashSet<Node> has = new HashSet<Node>();
	                	for (Node t : ss.get(1)) {
	                		Map<String, Object> config = t.getConfig();
	                		if (config != null && config.get(Constants.Ref) != null) {
	                			String s = config.get(Constants.Ref).toString().split("\\*")[0];
	                			if (schemas.containsKey(s)) has.add(schemas.get(s));
	                		}
	                	}
	    				ObjectNode jsonObject = om.createObjectNode();
	                    boolean isExistsMysql = false;
	                    boolean isExistsPostgres = false;
	                    boolean isExistsMain = false;
	                    boolean isExistsHive = false;
	                    boolean isExistsJava = false;
	                    boolean isExistsClone = false;
	                    ArrayNode schemaJSONArray = om.createArrayNode();
	                    for(Node node : has) {
	                    	Map<String, Object> config = node.getConfig();
	                        ObjectNode schemaNode = om.createObjectNode();
	                        schemaNode.put(Constants.Name, node.getName());
	                        schemaNode.put(Constants.Type, "custom");
	                        ObjectNode operandNode = om.createObjectNode();
	                        Schema schema = Schema.fromValue(config.get(Constants.Type));
	                        switch (schema) {
		                        case POSTGRES:
		                        case GREENPLUM: schemaNode.put("factory", "org.apache.calcite.adapter.jdbc.JdbcSchema$Factory");
		                            operandNode.put("jdbcDriver", "org.postgresql.Driver");
		                            isExistsPostgres = true;
		                            break;
		                        case MYSQL: schemaNode.put("factory", "org.apache.calcite.adapter.jdbc.JdbcSchema$Factory");
		                            operandNode.put("jdbcDriver", "com.mysql.jdbc.Driver");
		                            isExistsMysql = true;
		                            break;
		                        case HIVE: schemaNode.put("factory", "org.apache.calcite.adapter.jdbc.JdbcSchema$Factory");
		                            operandNode.put("jdbcDriver", "org.apache.hive.jdbc.HiveDriver");
		                            isExistsHive = true;
		                            break;
		                        case JAVA: schemaNode.put("factory", "org.apache.calcite.adapter.java.ReflectiveSchema$Factory");
		                        	isExistsJava = true;
		                        	break;
		                        case CLONE: schemaNode.put("factory", "org.apache.calcite.adapter.clone.CloneSchema$Factory");
		                        	isExistsClone = true;
		                        	break;
		                        default: schemaNode.put("factory", "net.yeeyaa.perception.search.calcite.Factory");
		                            operandNode.put("dataset", dataset);
		                            isExistsMain = true;
	                        }
                            for (Entry<String, Object> entry : config.entrySet()) operandNode.put(entry.getKey(), entry.getValue().toString());
	                        schemaNode.set("operand", operandNode);
	                        schemaJSONArray.add(schemaNode);
	                    }
	                    if (isExistsMain) jsonObject.put("defaultSchema", Schema.CARBON.name());
	                    else if (isExistsHive) jsonObject.put("defaultSchema", Schema.HIVE.name());
	                    else if (isExistsPostgres) jsonObject.put("defaultSchema", Schema.POSTGRES.name());
	                    else if (isExistsMysql) jsonObject.put("defaultSchema", Schema.MYSQL.name());
	                    else if (isExistsJava) jsonObject.put("defaultSchema", Schema.JAVA.name());
	                    else if (isExistsClone) jsonObject.put("defaultSchema", Schema.CLONE.name());
	                    jsonObject.set("schemas", schemaJSONArray);
	                    value = "inline:" + om.writeValueAsString(jsonObject);
	                    cache.put("search.schema." + dataset, value);
	                }
				} catch (Exception e) {
                	log.error("search schema fail.", e);
                }
				if (value == null) return null;
				else {
					HashMap<String, Object> ret = new HashMap<String, Object>(2);
	                ret.put("model", value);
	        		return ret;
				}
			}
		};
	}
	
    public static Type getType(String value) {
    	Type type =  Type.fromValue(value);
    	if (type == null) {
    		for (Type t : Type.values()) if (value.indexOf(t.value()) == 0) return t;
    		return Type.STRING;
    	} else return type;
	}
	
	public static enum Type {
        BYTE("byte"),
        SHORT("short"),
        INTEGER("integer"),
        LONG("long"),
        FLOAT("float"),
        DOUBLE("double"),
        DECIMAL("decimal"),
        STRING("string"),
        BINARY("binary"),
        BOOLEAN("boolean"),
        TIMESTAMP("timestamp"),
        DATE("date"),
        ARRAY("array"),
        MAP("map"),
        DICT("dict"),
        BITMAP("bitmap"),
        OBJECT("object");

        protected final String value;
        protected static final Map<String, Type> CONSTANTS = new HashMap<String, Type>();

        private Type(String value) {
            this.value = value;
        }

        public String toString() {
            return this.value;
        }

        public String value() {
            return this.value;
        }

        public static Type fromValue(String value) {
            Type constant = CONSTANTS.get(value);
            if (constant == null) return null;
            else return constant;
        }

        static {
        	for (Type c : values()) CONSTANTS.put(c.value, c);
        }
    }
	
    public static enum Schema {
        HDFS("hdfs"),
        MYSQL("mysql"),
        HIVE("hive"),
        POSTGRES("postgres"),
        GREENPLUM("greenplum"),
        JAVA("java"),
        DRUID("druid"),
        ELASTIC_SEARCH("es"),
        FILE("file"),
        REDIS("redis"),
        KAFKA("kafka"),
        CLONE("clone"),
        HBASE("hbase"),
        CARBON("carbon"),
        KUDU("kudu"),
        OTHER("other");

        protected final String value;
        protected static final Map<String, Schema> CONSTANTS = new HashMap<String, Schema>();

        private Schema(String value) {
            this.value = value;
        }

        public String toString() {
            return this.value;
        }

        public String value() {
            return this.value;
        }
        
        public static Schema fromValue(Object value) {
        	Schema constant = CONSTANTS.get(value);
            if (constant == null) return OTHER;
            else return constant;
        }

        static {
           	for (Schema c : values()) CONSTANTS.put(c.value, c);
        }
    }
}
