package net.yeeyaa.perception.search.schema;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.yeeyaa.eight.IBiProcessor;
import net.yeeyaa.eight.IListableTransaction;
import net.yeeyaa.eight.IProcessor;
import net.yeeyaa.eight.IResource;
import net.yeeyaa.eight.ITransactionResource;
import net.yeeyaa.eight.ITriProcessor;
import net.yeeyaa.perception.search.schema.Node.Category;
import net.yeeyaa.perception.search.schema.util.GZipUtils;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Generator implements IProcessor<Boolean, Boolean>, IBiProcessor<Object, String, String> {
    protected final Logger log;
    protected final IBiProcessor<String, Class<?>, Object> marshal;
    protected IProcessor<Object, String> unmarshal;
	protected IListableTransaction<String, Object, IResource<String, Object>, Object> meta;
	protected ITransactionResource<String, Object, IResource<String, Object>, Object> flag;
	protected ITriProcessor<String, String, Map<String, Object>, Object> info;
    protected IProcessor<Object, Node[]> holder;
	protected ITriProcessor<Boolean, String, Object, Object> convertor;
	protected ExecutorService executor;
	protected Integer duration; // max generate time < this < redo interval
	protected Integer bucketSize = 1000;
	protected Integer listSize = 5;
    protected Node[] config;
    protected String digest;
    
	public Generator(IBiProcessor<String, Class<?>, Object> marshal) {
		this.log = LoggerFactory.getLogger(Generator.class);
		this.marshal = marshal;
	}

	public Generator(Logger log, IBiProcessor<String, Class<?>, Object> marshal) {
		this.log = log == null ? LoggerFactory.getLogger(Generator.class) : log;
		this.marshal = marshal;
	}

	public void setHolder(IProcessor<Object, Node[]> holder) {
		this.holder = holder;
	}

	public void setUnmarshal(IProcessor<Object, String> unmarshal) {
		this.unmarshal = unmarshal;
	}

	public void setExecutor(ExecutorService executor) {
		this.executor = executor;
	}

	public void setMeta(IListableTransaction<String, Object, IResource<String, Object>, Object> meta) {
		this.meta = meta;
	}

	public void setFlag(ITransactionResource<String, Object, IResource<String, Object>, Object> flag) {
		this.flag = flag;
	}

	public void setInfo(ITriProcessor<String, String, Map<String, Object>, Object> info) {
		this.info = info;
	}
	
	public void setBucketSize(Integer bucketSize) {
		if (bucketSize != null && bucketSize > 0) this.bucketSize = bucketSize;
	}

	public void setListSize(Integer listSize) {
		if (listSize != null && listSize > 0) this.listSize = listSize;
	}

	public void setDuration(Integer duration) {
		this.duration = duration;
	}

	public void setConvertor(ITriProcessor<Boolean, String, Object, Object> convertor) {
		this.convertor = convertor;
	}

	public void setConfig(String config) {
		if (config != null) try {
			config = config.startsWith("[") ? config : GZipUtils.decompress(config);
			this.config = (Node[]) marshal.perform(config, Node[].class);
			this.digest = new Integer(new ToStringBuilder(null).append(this.config).toString().hashCode()).toString();
		} catch (Exception e) {
			log.error("set config fail.", e);
		}
	}
    
	protected class Cache {
		protected final Map<String, Set<String>> alter;
		protected final Set<String> exist;
		protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		protected final Map<String, Map<String, Node>> cache = new ConcurrentHashMap<String, Map<String, Node>>(bucketSize * listSize * 2);
		protected final Set<String>[] buckets = new Set[listSize];
		protected volatile Integer index = 0;

		public Cache(Set<String> exist, Map<String, Set<String>> alter){
			this.exist = exist;
			this.alter = alter;
			for(int i = 0; i < listSize; i++) buckets[i] = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
		}
		
		public Map<String, Map<String, Node>> close(){
			return cache;
		}
		
		protected synchronized Set<String> lru(){
			if(buckets[index].size() > bucketSize){
				Integer i = (index + 1) % listSize;
				final Set<String> set = buckets[i];
				if(set.size() >= bucketSize) {
					buckets[i] = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
					Runnable run = new Runnable(){
						@Override
						public void run() {
							Iterator<String> itr = set.iterator();
							while(itr.hasNext()) {
								Object key = itr.next();
								for(Set<String> bucket : buckets) if(bucket.contains(key)){
									itr.remove();
									break;
								}
							}
							if (set.size() > 0) {
								final Map<String, Node[]> kv = new HashMap<String, Node[]>(set.size() * 2);
								for (String key : set) {
									Map<String, Node> value = cache.get(key);
									if (value != null) kv.put(key, value.values().toArray(new Node[value.size()]));
								}
								meta.store(kv);
								lock.writeLock().lock();
								try {
									for (String key : kv.keySet()) {
										exist.add(key);
										boolean remove = true;
										for(Set<String> bucket : buckets) if(bucket.contains(key)) {
											remove = false;
											break;
										}
										if (remove) cache.remove(key);
									}
								} finally {
									lock.writeLock().unlock();
								}
							}		
						}		
					};
					if (executor == null) run.run();
					else executor.execute(run);
				}
				index = i;
			}
			return buckets[index];
		}

		public Map<String, Node> find(String key) {
			lock.readLock().lock();
			try {
				lru().add(key);
			} finally {
				lock.readLock().unlock();
			}
			Map<String, Node> ret = cache.get(key);
			if(ret == null) synchronized(cache) {
				ret = cache.get(key);
				if (ret == null) {
					Object o = null;
					if (exist.contains(key)) {
						List<Node[]> ls = (List<Node[]>) meta.find(key);
						if (ls != null && ls.size() > 0) o = ls.get(0);
					}
					ret = new ConcurrentHashMap<String, Node>();
					Set<String> alt = alter.remove(key);
					if (o != null) if (alt == null) for (Node n : (Node[])o) ret.put(n.getName(), n);
					else for (Node n : (Node[])o) if (!alt.contains(n.getName())) ret.put(n.getName(), n);
					cache.put(key, ret);
				}
			}
			return ret;
		}
	}

	protected class Runner implements Runnable {
		protected final Boolean mode;
		
		public Runner(Boolean mode) {
			this.mode = mode;
		}

		@Override
		public void run() {
			process(mode);
		}
	}
	
	@Override
    public String perform(Object type, String content) {
		if (content == null) if (type == null || type instanceof Boolean) if (executor == null) process((Boolean) type);
		else executor.execute(new Runner((Boolean) type));
		else {
			Object fg = flag == null ? Constants.Empty : flag.find(Constants.Flag);
			if (fg != null && Long.parseLong(fg.toString()) < 0) try {
				Map<String[], Object> all = meta.all();
				Map<String, Object> config = new HashMap<String, Object>(all.size() * 2);
				for (Entry<String[], Object> entry : all.entrySet()) config.put(entry.getKey()[0], entry.getValue());
				String cfg = unmarshal.process(config);
				if (cfg != null) return type instanceof String ? cfg : GZipUtils.compress(cfg);
			} catch (Exception e) {
				log.error("get config fail.", e);
			}
		} else if (type == null) flag.store(content, Constants.Alter);
		else if (type instanceof String) flag.store(content, type.toString());
		else if (type instanceof Number) {
			flag.store(content, Constants.Alter);
			if (executor == null) process(null);
			else executor.execute(new Runner(null));
		} else if (type instanceof Collection) {
			flag.store(content, Constants.Alter);
			if (executor == null) process(false);
			else executor.execute(new Runner(false));
		} else if (!(type instanceof Boolean)) {
			flag.store(content, Constants.Alter);
			if (executor == null) process(true);
			else executor.execute(new Runner(true));
		}  else if ((Boolean) type) flag.store(content, Constants.Config);
		else {
			flag.store(content, Constants.Config);
			if (executor == null) process(null);
			else executor.execute(new Runner(null));
		}
		return null;
	}
	
	@Override
    public Boolean process(Boolean mode) { //mode -> null: amend; false: refresh; true: amend with re-fetch (for ids source self alter)
		final Boolean[] f = new Boolean[]{true};
		final Object status[] = new Object[]{null, null};
		if (flag != null) flag.execute(new IProcessor<IResource<String,Object>, Object>() {
			@Override
			public Object process(IResource<String, Object> resource) {
				Object fg = resource.find(Constants.Flag);
				status[0] = resource.find(Constants.Alter);
				status[1] = resource.find(Constants.Config);
				Long flag = fg == null ? 0 : Long.parseLong(fg.toString());
				if (flag < 0 && (status[0] != null || status[1] != null)) flag = 0L;
				Long now = System.currentTimeMillis();
				if (flag == 0 || duration == null && flag < 0 || duration != null && Math.abs(flag) + duration < now) {
					resource.discard(Constants.Alter);
					resource.discard(Constants.Config);
					resource.store(now.toString(), Constants.Flag);
				} else f[0] = false;
				return f[0];
			}
		});
		if (f[0]) try {
			if (status[1] == null) {
				Node[] config = holder == null ? this.config : holder.process(null);
				if (config == null) meta.empty();
				else if (config.length == 0) meta.execute(new IProcessor<IResource<String,Object>, Object>() {
					@Override
					public Object process(IResource<String, Object> resource) {
						resource.empty();
						resource.store(new Node[0], Constants.Root);
						return resource.store(new Node[]{new Node(Constants.Empty, Category.OTHER, null, null, null)}, Constants.Flag);
					}
				}); else {
		    		final Map<String, Node[]> confs = new HashMap<String, Node[]>();
					confs.put(Constants.Root, config);
		    		final Map<String, Map<String, Node>> schema = new HashMap<String, Map<String, Node>>();
		    		Map<String, Map<String, Node>> share = new HashMap<String, Map<String, Node>>();
		    		Map<String, Map<String, Node>> individual = new HashMap<String, Map<String, Node>>();
		    		for (Node n : config) switch(n.getCategory()) {
		    			case DATASET : if (n.getSub() != null && n.getSub().length > 0) for (Node s : n.getSub()) if (Category.VIEW.equals(s.getCategory())) 
			    			if (s.getConfig() == null || s.getConfig().get(Constants.Forbid) == null) {
			    				Map<String, Node> m = share.get(n.getName());
			    				if (m == null) {
			    					m = new HashMap<String, Node>();
			    					share.put(n.getName(), m);
			    				}
			    				m.put(s.getName(), s);
			    			} else {
			    				Map<String, Node> m = individual.get(n.getName());
			    				if (m == null) {
			    					m = new HashMap<String, Node>();
			    					individual.put(n.getName(), m);
			    				}
			    				m.put(s.getName(), s);
			    			}
		    				break;
		    			case SCHEMA : if (n.getSub() != null && n.getSub().length > 0) for (Node s : n.getSub()) if (Category.TABLE.equals(s.getCategory())) {
			    				Map<String, Node> m = schema.get(n.getName());
			    				if (m == null) {
			    					m = new HashMap<String, Node>();
			    					schema.put(n.getName(), m);
			    				}
			    				m.put(s.getName(), s);
			    			}
		    				break;
		    			default : confs.put(n.getCategory().value() + "*" + n.getName(), new Node[]{n});
		    		}
		    		Map<String, Map<String, Node>> datasets = new HashMap<String, Map<String, Node>>();
		    		for (Entry<String, Map<String, Node>> d : share.entrySet()) for (Entry<String, Node> v : d.getValue().entrySet()) {
		    			Map<String, Node> in = individual.get(d.getKey());
		    			if (in != null) {
		    				in.remove(v.getKey());
		    				if (in.size() == 0) individual.remove(d.getKey());
		    			}
		    			Node view = (Node) v.getValue().clone();
		    			Map<String, Node> views = datasets.get(d.getKey());
		    			if (views == null) {
		    				views = new HashMap<String, Node>();
		    				datasets.put(d.getKey(), views);
		    			}
		    			Map<String, Object> cfg = view.getConfig();
		    			if (cfg != null) {
		    				Object ref = cfg.get(Constants.Ref);
		    				if (ref != null) {
		    					String[] path = ref.toString().split("\\*");
		    					if (path.length > 0) {
		    						Map<String, Node> s = schema.get(path[0]);
		    						if (s != null) {
		    							Node t = s.get(path.length > 1 ? path[1] : view.getName());
		    							Object direct = cfg.get(Constants.Direct);//null: must have corresponding table and column,  false: all table columns in with merge, true: only view columns in with merge
		    							if (t != null) {
		    								if (t.getConfig() != null) {
		    									Object override = cfg.get(Constants.Override);//null: merge config view prefer; not bool: merge config table prefer; false: override by table config with ref; true: keep with view config
		    									if (override == null) {
			    									HashMap<String, Object> nc = new HashMap<String, Object>(t.getConfig());
			    									nc.putAll(cfg);
			    									view.setConfig(nc);
		    									} else if (!(override instanceof Boolean)) {
		    										HashMap<String, Object> nc = new HashMap<String, Object>(cfg);
			    									nc.putAll(t.getConfig());
			    									view.setConfig(nc);
		    									} else if (Boolean.FALSE.equals(override)) {
		    										HashMap<String, Object> nc = new HashMap<String, Object>(t.getConfig());
		    										nc.put(Constants.Ref, ref);
		    										view.setConfig(nc);
		    									}
		    								}
		    								if (t.getSub() != null && t.getSub().length > 0) {
		    									Map<String, Node> ps = new HashMap<String, Node>();
												Map<String, Node> sub = new HashMap<String, Node>();
												for (Node n : t.getSub()) {
													ps.put(n.getName(), n);
													if (Boolean.FALSE.equals(direct)) sub.put(n.getName(), n);
												}
												if (view.getSub() != null && view.getSub().length > 0) for (Node n : view.getSub()) {
													Map<String, Object> c = n.getConfig();
													Node o = ps.get(c == null || c.get(Constants.Ref) == null ? n.getName() : c.get(Constants.Ref));
													if (o != null) {
														Node nn = (Node) n.clone();
					    								if (o.getConfig() != null) {
					    									Object override = c == null ? null : c.get(Constants.Override);
					    									if (override == null) {
						    									HashMap<String, Object> nc = new HashMap<String, Object>(o.getConfig());
						    									nc.putAll(c);
						    									nn.setConfig(nc);
					    									} else if (!(override instanceof Boolean)) {
					    										HashMap<String, Object> nc = new HashMap<String, Object>(c);
						    									nc.putAll(o.getConfig());
						    									nn.setConfig(nc);
					    									} else if (Boolean.FALSE.equals(override)) if (c == null || c.get(Constants.Ref) == null) nn.setConfig(o.getConfig());
					    									else {
					    										HashMap<String, Object> nc = new HashMap<String, Object>(o.getConfig());
					    										nc.put(Constants.Ref, c.get(Constants.Ref));
					    										nn.setConfig(nc);
					    									}
					    								}
					    								if (o.getSub() != null && o.getSub().length > 0) if (nn.getSub() == null || nn.getSub().length == 0) nn.setSub(o.getSub());
					    								else {
					    									Node[] nns = Arrays.copyOf(o.getSub(), o.getSub().length + nn.getSub().length);
					    									for (int i = 0; i < nn.getSub().length; i++) nns[o.getSub().length + i] = nn.getSub()[i];
					    									nn.setSub(nns);
					    								}
					    								sub.put(nn.getName(), nn);
													} else if (direct != null) sub.put(n.getName(), n);
												}
												if (sub.size() > 0) {
													view.setSub(sub.values().toArray(new Node[sub.size()]));
				    								views.put(v.getKey(), view);
												}
		    								} else if (direct != null && view.getSub() != null && view.getSub().length > 0) views.put(v.getKey(), view);
		    							} else if (direct != null && view.getSub() != null && view.getSub().length > 0) views.put(v.getKey(), view);
		    						}
		    					}
		    				}
		    			}
		    		}
					for (Entry<String, Map<String, Node>> entry : datasets.entrySet()) {
						Collection<Node> ns = entry.getValue().values();
						confs.put(Category.DATASET.value() + "*" + entry.getKey(), ns.toArray(new Node[ns.size()]));
					}
		    		if (individual.size() == 0) {
						confs.put(Constants.Flag, new Node[]{new Node(holder == null ? digest : new Integer(new ToStringBuilder(null).append(config).toString().hashCode()).toString(), Category.OTHER, null, null, null)});
		    			meta.execute(new IProcessor<IResource<String,Object>, Object>() {
							@Override
							public Object process(IResource<String, Object> resource) {
								resource.empty();
								for (Entry<String, Node[]> entry : confs.entrySet()) resource.store(entry.getValue(), entry.getKey());
								return null;
							}
					}); } else {
		    			Node[] o = null;
		    			if (Boolean.FALSE.equals(mode)) meta.empty();
		    			else {
		    				List<Node[]> ls = (List<Node[]>) meta.find(Constants.Root);
		    				if (ls != null && ls.size() > 0) o = ls.get(0);
		    			}
		        		final Map<String, Map<String, Entry<Node, Boolean>>> oschema = new HashMap<String, Map<String, Entry<Node, Boolean>>>();
		        		Map<String, Set<String>> oshare = new HashMap<String, Set<String>>();
		        		final Map<String, Map<String, Entry<Node, Boolean>>> oindividual = new HashMap<String, Map<String, Entry<Node, Boolean>>>();
		        		final Map<String, Set<String>> alter = new HashMap<String, Set<String>>();
		        		Set<String> exist  = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
		        		final Set<String> other = new HashSet<String>();
		    			f[0] = o == null || o.length != config.length; 
		    			if (o != null && o.length > 0) for (int i = 0; i < o.length; i++) {
		    				if (!f[0]) f[0] = !o[i].equals(config[i]);
		    				switch(o[i].getCategory()) {
			        			case DATASET : if (o[i].getSub() != null && o[i].getSub().length > 0) for (Node s : o[i].getSub()) if (Category.VIEW.equals(s.getCategory())) 
			    	    			if (s.getConfig() == null || s.getConfig().get(Constants.Forbid) == null) {
			    	    				Set<String> m = oshare.get(o[i].getName());
			    	    				if (m == null) {
			    	    					m = new HashSet<String>();
			    	    					oshare.put(o[i].getName(), m);
			    	    				}
			    	    				m.add(s.getName());
			    	    			} else {
			    	    				Map<String, Entry<Node, Boolean>> m = oindividual.get(o[i].getName());
			    	    				if (m == null) {
			    	    					m = new HashMap<String, Entry<Node, Boolean>>();
			    	    					oindividual.put(o[i].getName(), m);
			    	    				}
			    	    				m.put(s.getName(), new SimpleEntry<Node, Boolean>(s,true));
			    	    			}
			        				break;
			        			case SCHEMA : if (o[i].getSub() != null && o[i].getSub().length > 0) for (Node s : o[i].getSub()) if (Category.TABLE.equals(s.getCategory())) {
			    	    				Map<String, Entry<Node, Boolean>> m = oschema.get(o[i].getName());
			    	    				if (m == null) {
			    	    					m = new HashMap<String, Entry<Node, Boolean>>();
			    	    					oschema.put(o[i].getName(), m);
			    	    				}
			    	    				if (f[0]) {
				    	    				Map<String, Node> ns = schema.get(o[i].getName());
				    	    				Node nn = ns == null ? null : ns.get(s.getName());
				    	    				m.put(s.getName(), new SimpleEntry<Node, Boolean>(s, nn == null ? null : s.equals(nn)));
			    	    				} else m.put(s.getName(), new SimpleEntry<Node, Boolean>(s, true));
			    	    			}
			        				break;
			        			default : if (f[0] && confs.get(o[i].getCategory().value() + "*" + o[i].getName()) == null) other.add(o[i].getCategory().value() + "*" + o[i].getName());
			        		}
		    			}
	    				HashSet<String> remove = new HashSet<String>();
		    			if (status[0] != null) for (String s : status[0].toString().split("\\*\\*")) remove.add(s);
		    			if (f[0]) {
		    				if (oshare.size() > 0) {
			    				Iterator<Entry<String, Set<String>>> di = oshare.entrySet().iterator();
			    				while (di.hasNext()) {
			    					Entry<String, Set<String>> d = di.next();
			    					for (String v : d.getValue()) {
				    					Map<String, Entry<Node, Boolean>> m = oindividual.get(d.getKey());
				    					if (m != null) {
				    						m.remove(v);
				    						if (m.size() == 0) oindividual.remove(d.getKey());
				    					}
			    					}
			    					if (share.get(d.getKey()) == null) di.remove();
			    				}
		    				}
			    			if (oindividual.size() > 0) {
			    				Iterator<Entry<String, Map<String, Entry<Node, Boolean>>>> di = oindividual.entrySet().iterator();
			    				while (di.hasNext()) {
			    					Entry<String, Map<String, Entry<Node, Boolean>>> d = di.next();
			    					Iterator<Entry<String, Entry<Node, Boolean>>> vi = d.getValue().entrySet().iterator();
			    					while (vi.hasNext()) {
			    						Entry<String, Entry<Node, Boolean>> v = vi.next();
			    						Node old = v.getValue().getKey();
			    	    				Map<String, Node> ni = individual.get(d.getKey());
			    	    				Node nn = ni == null ? null : ni.get(v.getKey());
			    	    				Boolean eq = nn == null ? null : old.equals(nn);
			        					if (eq == null) vi.remove();
			        					else if (eq) {
			        						Map<String, Object> cfg = old.getConfig();
			        						if (cfg != null && cfg.get(Constants.Ref) != null) {
			        	    					String[] path = cfg.get(Constants.Ref).toString().split("\\*");
			        	    					if (path.length > 0) {
			        	    						Map<String, Entry<Node, Boolean>> s = oschema.get(path[0]);
			        	    						if (s == null) {
			        	    							Map<String, Node> ns = schema.get(path[0]);
			        	    							if (ns != null && ns.get(path.length > 1 ? path[1] : old.getName()) != null) v.getValue().setValue(false);
			        	    						} else {
			        	    							Entry<Node, Boolean> t = s.get(path.length > 1 ? path[1] : old.getName());
			        	    							if (t == null) {
			            	    							Map<String, Node> ns = schema.get(path[0]);
			            	    							if (ns != null && ns.get(path.length > 1 ? path[1] : old.getName()) != null) v.getValue().setValue(false);
			        	    							} else if (!Boolean.TRUE.equals(t.getValue())) v.getValue().setValue(false);
			        	    						} 
			        	    					}
			        						}
			            				} else v.getValue().setValue(false);
		        						Map<String, Object> temp = old.getTemp();
		        						Boolean alt = !Boolean.TRUE.equals(v.getValue().getValue());
		        						if (temp != null && temp.get(Constants.Id) instanceof Collection) {
		        							Iterator<Object> itr = ((Collection<Object>) temp.get(Constants.Id)).iterator();
		        							while (itr.hasNext()) {
		        								Object id = itr.next().toString();
		            							String key = Category.DATASET.value() + "*" + d.getKey() + "*" + id;
		            							if (alt || remove.contains(id) || remove.contains(id + "*" + d.getKey()) || remove.contains(id + "*" + d.getKey() + "*" + v.getKey())) {
		    	        							Set<String> tabs = alter.get(key);
		    	        							if (tabs == null) {
		    	        								tabs = new HashSet<String>();
		    	        								alter.put(key, tabs);
		    	        							}
		    	        							tabs.add(v.getKey());
		    	        							exist.add(key);
		    	        							itr.remove();
		    	        						} else if (Boolean.TRUE.equals(mode)) exist.add(key);
		        							}
		        						}
			    					}
			    					if (d.getValue().size() == 0) di.remove();
			    				}
		    				}	
		    			} else {
		    				oshare = Collections.EMPTY_MAP;
		    				if (oindividual.size() > 0 && remove.size() > 0) for(Entry<String, Map<String, Entry<Node, Boolean>>> d : oindividual.entrySet()) for (Entry<String, Entry<Node, Boolean>> v : d.getValue().entrySet()) {
		    					Map<String, Object> temp = v.getValue().getKey().getTemp();
		    					if (temp != null && temp.get(Constants.Id) instanceof Collection) {
		    						Iterator<Object> itr = ((Collection<Object>) temp.get(Constants.Id)).iterator();
	    							while (itr.hasNext()) {
	    								Object id = itr.next().toString();
	        							String key = Category.DATASET.value() + "*" + d.getKey() + "*" + id;
	        							if (remove.contains(id) || remove.contains(id + "*" + d.getKey()) || remove.contains(id + "*" + d.getKey() + "*" + v.getKey())) {
		        							Set<String> tabs = alter.get(key);
		        							if (tabs == null) {
		        								tabs = new HashSet<String>();
		        								alter.put(key, tabs);
		        							}
		        							tabs.add(v.getKey());
		        							exist.add(key);
		        							itr.remove();
		        						} else if (Boolean.TRUE.equals(mode)) exist.add(key);
	    							}
		    					}
		    				}
		    			}
		    			final Cache cache = new Cache(exist, alter);
		    			List<Future<?>> futures = new LinkedList<Future<?>>();
		    			for (final Entry<String, Map<String, Node>> d : individual.entrySet()) for (final Entry<String, Node> v : d.getValue().entrySet()) {
		    				Runnable run = new Runnable() {
								@Override
								public void run() {
		    						Node view = (Node) v.getValue().clone();
		    		    			Map<String, Object> cfg = view.getConfig();
		    		    			Object direct = null;
		    		    			String key = d.getKey() + "*" + v.getKey();
		    		    			HashMap<String, Node> sub = new HashMap<String, Node>();
		    		    			HashMap<String, Node> ps = new HashMap<String, Node>();
		    		    			if (cfg != null) {
		    		    				Object ref = cfg.get(Constants.Ref);
		    		    				if (ref != null) {
		    		    					String[] path = ref.toString().split("\\*");
		    		    					if (path.length > 0) {
		    		    						Map<String, Node> s = schema.get(path[0]);
		    		    						if (s != null) {
		    		    							Node t = s.get(path.length > 1 ? path[1] : view.getName());
		    		    							direct = cfg.get(Constants.Direct);
		    		    							if (t != null) {
		    		    								if (t.getConfig() != null) {
		    		    									Object override = cfg.get(Constants.Override);
		    		    									if (override == null) {
		    			    									HashMap<String, Object> nc = new HashMap<String, Object>(t.getConfig());
		    			    									nc.putAll(cfg);
		    			    									view.setConfig(nc);
		    		    									} else if (!(override instanceof Boolean)) {
		    		    										HashMap<String, Object> nc = new HashMap<String, Object>(cfg);
		    			    									nc.putAll(t.getConfig());
		    			    									view.setConfig(nc);
		    		    									} else if (Boolean.FALSE.equals(override)) {
		    		    										HashMap<String, Object> nc = new HashMap<String, Object>(t.getConfig());
		    		    										nc.put(Constants.Ref, ref);
		    		    										view.setConfig(nc);
		    		    									}
		    		    								}
		    		    								if (t.getSub() != null && t.getSub().length > 0) {
		    												for (Node n : t.getSub()) {
		    													ps.put(n.getName(), n);
		    													if (Boolean.FALSE.equals(direct)) sub.put(n.getName(), n);
		    												}
		    												if (view.getSub() != null && view.getSub().length > 0) for (Node n : view.getSub()) {
		    													Map<String, Object> c = n.getConfig();
		    													Node o = ps.get(c == null || c.get(Constants.Ref) == null ? n.getName() : c.get(Constants.Ref));
		    													if (o != null) {
		    														Node nn = (Node) n.clone();
		    					    								if (o.getConfig() != null) {
		    					    									Object ov = c == null ? null : c.get(Constants.Override);
		    					    									if (ov == null) {
		    						    									HashMap<String, Object> nc = new HashMap<String, Object>(o.getConfig());
		    						    									nc.putAll(c);
		    						    									nn.setConfig(nc);
		    					    									} else if (!(ov instanceof Boolean)) {
		    					    										HashMap<String, Object> nc = new HashMap<String, Object>(c);
		    						    									nc.putAll(o.getConfig());
		    						    									nn.setConfig(nc);
		    					    									} else if (Boolean.FALSE.equals(ov)) if (c == null || c.get(Constants.Ref) == null) nn.setConfig(o.getConfig());
		    					    									else {
		    					    										HashMap<String, Object> nc = new HashMap<String, Object>(o.getConfig());
		    					    										nc.put(Constants.Ref, c.get(Constants.Ref));
		    					    										nn.setConfig(nc);
		    					    									}
		    					    								}
		    					    								if (o.getSub() != null && o.getSub().length > 0) if (nn.getSub() == null || nn.getSub().length == 0) nn.setSub(o.getSub());
		    					    								else {
		    					    									Node[] nns = Arrays.copyOf(o.getSub(), o.getSub().length + nn.getSub().length);
		    					    									for (int i = 0; i < nn.getSub().length; i++) nns[o.getSub().length + i] = nn.getSub()[i];
		    					    									nn.setSub(nns);
		    					    								}
		    					    								sub.put(nn.getName(), nn);
		    													} else if (direct != null) sub.put(n.getName(), n);
		    												}
		    		    								}
		    		    							}
		    		    						}
		    		    					}
		    		    				}
		    		    			}
		    		    			if (direct != null || ps.size() > 0) {
										Entry<Node, Boolean> old = null;
					    				Map<String, Entry<Node, Boolean>> m = oindividual.get(d.getKey());
					    				if (m != null) old = m.get(v.getKey());
					    				Collection<Object> done = old != null && old.getValue() && old.getKey().getTemp() != null ? (Collection<Object>) old.getKey().getTemp().get(Constants.Id) : null;
					    				HashSet<Object> idone = done == null ? new HashSet<Object>() : new HashSet<Object>(done);
					    				Collection<Object> permit = (Collection<Object>) v.getValue().getConfig().get(Constants.Permit);
					    				Map<String, Object> temp = v.getValue().getTemp();
					    				if (temp == null) {
					    					temp = new HashMap<String, Object>();
					    					v.getValue().setTemp(temp);
					    				}
					    				temp.put(Constants.Id, idone);
				    					HashSet<Object> ids = new HashSet<Object>();
					    				if (permit == null) {
					    					if (convertor == null) {
						    					ids.addAll((Collection<Object>) v.getValue().getConfig().get(Constants.Forbid));
						    					ids.addAll(idone);
					    					} else {
					    						for (Object id : (Collection<Object>) v.getValue().getConfig().get(Constants.Forbid)) ids.add(convertor.operate(true, key, id));
					    						for (Object id : idone) ids.add(convertor.operate(true, key, id));
					    					}
					    				} else {
					    					if (convertor == null) {
						    					ids.addAll(permit);
						    					ids.removeAll(idone);
					    					} else {
					    						for (Object id : permit) ids.add(convertor.operate(true, key, id));
					    						for (Object id : idone) ids.remove(convertor.operate(true, key, id));
					    					}
					    				}
				    			    	Map<String, Object> map = new HashMap<String, Object>();
				    			    	map.put(Constants.Id, ids);
					    				List<Map<String, Object>> result = permit == null ? (List<Map<String, Object>>) info.operate(key, Constants.Forbid, map) : ids.size() == 0 ? Collections.EMPTY_LIST : (List<Map<String, Object>>) info.operate(key, Constants.Permit, map);
					    				if (result != null && result.size() > 0) {
					    					Map<Object, Set<Map<String, Object>>> tabs = new HashMap<Object, Set<Map<String, Object>>>();
					    					for (Map<String, Object> paras : result) {
					    						Object id = paras.remove(Constants.Id);
					    						if (id != null) {
					    							if (convertor != null) id = convertor.operate(false, key, id);
					    							Set<Map<String, Object>> tab = tabs.get(id);
					    							if (tab == null) {
					    								tab = new HashSet<Map<String, Object>>();
					    								tabs.put(id, tab);
					    							}
					    							tab.add(paras);
					    						}
					    					}
					    					if (tabs.size() > 0) for (Entry<Object, Set<Map<String, Object>>> entry : tabs.entrySet()) {
					    						HashMap<String, Node> cols = (HashMap<String, Node>)sub.clone();
					    						for (Map<String, Object> col : entry.getValue()) {
					    							Object name = col.remove(Constants.Name);
					    							if (name != null) {
					    								Node n = cols.get(name);
					    								if (n == null) {
					    									n = new Node(name.toString(), Category.ITEM, col, null, null);
	    													Node o = ps.get(col.get(Constants.Ref) == null ? n.getName() : col.get(Constants.Ref));
	    													if (o != null) {
	    					    								if (o.getConfig() != null) {
	    					    									Object ov = col == null ? null : col.get(Constants.Override);
	    					    									if (ov == null) {
	    						    									HashMap<String, Object> nc = new HashMap<String, Object>(o.getConfig());
	    						    									nc.putAll(col);
	    						    									n.setConfig(nc);
	    					    									} else if (!(ov instanceof Boolean)) {
	    					    										HashMap<String, Object> nc = new HashMap<String, Object>(col);
	    						    									nc.putAll(o.getConfig());
	    						    									n.setConfig(nc);
	    					    									} else if (Boolean.FALSE.equals(ov)) if (col.get(Constants.Ref) == null) n.setConfig(o.getConfig());
	    					    									else {
	    					    										HashMap<String, Object> nc = new HashMap<String, Object>(o.getConfig());
	    					    										nc.put(Constants.Ref, col.get(Constants.Ref));
	    					    										n.setConfig(nc);
	    					    									}
	    					    								}
	    					    								if (o.getSub() != null && o.getSub().length > 0) n.setSub(o.getSub());
	    					    								cols.put(name.toString(), n);
	    													} else if (direct != null) cols.put(n.getName(), n);
					    								} else {
					    									n = (Node) n.clone();
					    									Map<String, Object> c = n.getConfig() == null ? col : new HashMap<String, Object>(n.getConfig());
					    									if (c != col) c.putAll(col);
					    									n.setConfig(c);
					    								}
					    							}
					    						}
					    						if (cols.size() > 0) {
						    						Node vw = (Node) view.clone();
						    						vw.setSub(cols.values().toArray(new Node[cols.size()]));
						    						cache.find(Category.DATASET.value() + "*" + d.getKey() + "*" + entry.getKey()).put(v.getKey(), vw);
						    						idone.add(entry.getKey());
					    						}
					    					}
					    				}
		    		    			}
								}
		    				};
							if (executor == null) run.run();
							else futures.add(executor.submit(run));
		    			}
		    			for (Future<?> future : futures) future.get();
		    			Map<String, Map<String, Node>> reserve = cache.close();
		    			if (reserve.size() > 0) {
		    				f[0] = true;
		    				for (Entry<String, Map<String, Node>> entry : reserve.entrySet()) {
								Collection<Node> ns = entry.getValue().values();
								confs.put(entry.getKey(), ns.toArray(new Node[ns.size()]));
			    			}
		    			}
		    			for (String rm : other) alter.put(rm, null);
		    			for (String rm : oshare.keySet()) alter.put(Category.DATASET.value() + "*" + rm, null);
		    			if (f[0] || alter.size() > 0) {
							confs.put(Constants.Flag, new Node[]{new Node((holder == null ? digest : new ToStringBuilder(null).append(config).toString().hashCode()) + "*" + UUID.randomUUID(), Category.OTHER, null, null, null)});
		    				meta.execute(new IProcessor<IResource<String,Object>, Object>() {
							@Override
							public Object process(IResource<String, Object> resource) {
								if (alter.size() > 0) resource.discard(alter.keySet().toArray(new String[alter.size()]));
								if (f[0]) for (Entry<String, Node[]> entry : confs.entrySet()) resource.store(entry.getValue(), entry.getKey());
								return null;
							}
		    			});}
		    			for (Map<String, Node> d : individual.values()) for (Node v : d.values()) {
		    				Map<String, Object> temp = v.getTemp();
		    				if (temp != null) temp.remove(Constants.Id);
		    			}
		    		}
		    	} 
			} else {
	    		final Map<String, Node[]> confs = (Map<String, Node[]>) marshal.perform(status[1].toString().startsWith("{") ? status[1].toString() : GZipUtils.decompress(status[1].toString()), null);
	    		meta.execute(new IProcessor<IResource<String,Object>, Object>() {
					@Override
					public Object process(IResource<String, Object> resource) {
						resource.empty();
						for (Entry<String, Node[]> entry : confs.entrySet()) resource.store(entry.getValue(), entry.getKey());
						return null;
					}
	    		});
	    	}
	    	if (flag != null) flag.store(new Long(-System.currentTimeMillis()).toString(), Constants.Flag);
	    	return true;
		} catch (Exception e) {
			if (flag != null) flag.store("0", Constants.Flag);
			log.error("create config fail.", e);
			return false;
		} else return null;
    }
}
