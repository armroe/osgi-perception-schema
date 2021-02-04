package net.yeeyaa.perception.search.calcite.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;


@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder({"name", "category", "config", "sub","temp"})
public class Node {
    @JsonProperty("name")
    protected String name;
    @JsonProperty("category")
    protected Category category;
    @JsonProperty("config")
    protected Map<String, Object> config;
    @JsonProperty("sub")
    protected Node[] sub;    
    @JsonProperty("temp")
    protected Map<String, Object> temp;
    
    public Node(String name, Category category, Map<String, Object> config, Node[] sub,	Map<String, Object> temp) {
		this.name = name;
		this.category = category;
		this.config = config;
		this.sub = sub;
		this.temp = temp;
	}

	public Node() {}

	public String getName() {
        return this.name;
    }
    
    public void setName(String name) {
        this.name = name;
    }

    public Category getCategory() {
        return this.category;
    }

    public void setCategory(Category category) {
        this.category = category;
    }

    public Map<String, Object> getConfig() {
		return config;
	}

	public void setConfig(Map<String, Object> config) {
		this.config = config;
	}

	public Node[] getSub() {
		return sub;
	}

	public void setSub(Node[] sub) {
		this.sub = sub;
	}

	public Map<String, Object> getTemp() {
		return temp;
	}

	public void setTemp(Map<String, Object> temp) {
		this.temp = temp;
	}

	public String toString() {
        return new ToStringBuilder(null).append("name", this.name).append("category", this.category).append("config", this.config).append("sub", this.sub).toString();
    }

    public int hashCode() {
        return new HashCodeBuilder().append(this.name).append(this.category).append(this.config).append(this.sub).toHashCode();
    }

    public boolean equals(Object other) {
        if (other == this) return true;
        else if (!(other instanceof Node)) return false;
        else {
            Node compare = (Node)other;
            return (new EqualsBuilder()).append(this.name, compare.name).append(this.category, compare.category).append(this.config, compare.config).append(this.sub, compare.sub).isEquals();
        }
    }

    @Override
	public Object clone() {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			return new Node(name, category, config, sub, temp);
		}
	}

	public static enum Category {
        SCHEMA("s"),
        COLUMN("c"),
        TABLE("t"),
        DATASET("d"),
        VIEW("v"),
        ITEM("i"),
        OTHER("o");

        protected final String value;
        protected static final Map<String, Category> CONSTANTS = new HashMap<String, Category>();
        static {
        	for (Category c : values()) CONSTANTS.put(c.value, c);
        }
        
        private Category(String value) {
            this.value = value;
        }

        public String toString() {
            return this.value;
        }

        @JsonValue
        public String value() {
            return this.value;
        }

        @JsonCreator
        public static Category fromValue(String value) {
            Category constant = CONSTANTS.get(value);
            if (constant == null) throw new IllegalArgumentException(value);
            else return constant;
        }
    }
}
