package net.yeeyaa.perception.search.calcite.schema.util;

import net.yeeyaa.eight.IBiProcessor;
import net.yeeyaa.eight.IProcessor;
import net.yeeyaa.eight.PlatformException;
import net.yeeyaa.perception.search.calcite.schema.SchemaError;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class JsonUtils<T> implements IProcessor<T, String>, IBiProcessor<String, Class<T>, T>{
	protected final ObjectMapper om;
    protected Boolean format;
    
    public JsonUtils(ObjectMapper om) {
		this.om = om == null ? new ObjectMapper().enableDefaultTyping(ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT) : om;
	}

	public JsonUtils() {
		this.om = new ObjectMapper().enableDefaultTyping(ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT);
	}

	public void setFormat(Boolean format) {
		this.format = format;
	}

	public static String formatJson(String jsonStr) {
        if (null == jsonStr || "".equals(jsonStr)) return "";
        StringBuilder sb = new StringBuilder();
        char last = '\0';
        char current = '\0';
        int indent = 0;
        for (int i = 0; i < jsonStr.length(); i++) {
            last = current;
            current = jsonStr.charAt(i);
            switch (current) {
                case '{':
                case '[':
                    sb.append(current);
                    sb.append('\n');
                    indent++;
                    addIndentBlank(sb, indent);
                    break;
                case '}':
                case ']':
                    sb.append('\n');
                    indent--;
                    addIndentBlank(sb, indent);
                    sb.append(current);
                    break;
                case ',':
                    sb.append(current);
                    if (last != '\\') {
                        sb.append('\n');
                        addIndentBlank(sb, indent);
                    }
                    break;
                default:
                    sb.append(current);
            }
        }
        return sb.toString();
    }
 
    protected static void addIndentBlank(StringBuilder sb, int indent) {
        for (int i = 0; i < indent; i++) sb.append('\t');
    }

	@Override
	public T perform(String content, Class<T> type) {
		if (content == null || content.trim().length() == 0) return null;
		else try {
			return type == null ? (T) om.readTree(content) : om.readValue(content, type);
		} catch (Exception e) {
			throw new PlatformException(SchemaError.ERROR_PARAMETERS, "marshal json fail.", e);
		}
	}

	@Override
	public String process(T object) {
		if (object == null) return null;
		else try {
			String ret = om.writeValueAsString(object);
			return Boolean.TRUE.equals(format) ? formatJson(ret) : ret;
		} catch (JsonProcessingException e) {
			throw new PlatformException(SchemaError.ERROR_PARAMETERS, "unmarshal json fail.", e);
		}
	}
}
