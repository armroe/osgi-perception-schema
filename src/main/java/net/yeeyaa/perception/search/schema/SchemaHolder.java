package net.yeeyaa.perception.search.schema;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import net.yeeyaa.eight.IBiProcessor;
import net.yeeyaa.eight.IInputResource;
import net.yeeyaa.eight.IProcessor;
import net.yeeyaa.eight.ITriProcessor;
import net.yeeyaa.eight.PlatformException;
import net.yeeyaa.eight.core.util.ConcurrentWeakIdentityHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;


public class SchemaHolder implements IProcessor<Object, Boolean>, IBiProcessor<String, Object[], Object>, ITriProcessor<Object, String, Map<Object, Object>, ITriProcessor<Object, String, Object, Object>> {
	public enum Operate{getCache, get, sync, process, operate}
    protected final UUID id = UUID.randomUUID();
    protected final Logger log;
    protected volatile Map<Object, Object> cache;
	protected volatile CountDownLatch startSignal = new CountDownLatch(1);
	protected volatile String flag;
	protected ExecutorService executor;
    protected IInputResource<Object, Object>  meta;
    protected IProcessor<Object, Object> destroy;
    protected IProcessor<String, ITriProcessor<Object, IBiProcessor<String, Object[], Object>, Map<Object, Object>, ITriProcessor<Object, String, Object, Object>>> factory;
    protected Integer size;//0:no limit
    protected Long timeout;//0:disable wait, >0: wait and throw exception when timeout; <0: wait with timeout; null: wait without timeout
	
	public SchemaHolder() {
		this.log = LoggerFactory.getLogger(SchemaHolder.class);
	}

	public SchemaHolder(Logger log) {
		this.log = log == null ? LoggerFactory.getLogger(SchemaHolder.class) : log;
	}

	public void setFactory(IProcessor<String, ITriProcessor<Object, IBiProcessor<String, Object[], Object>, Map<Object, Object>, ITriProcessor<Object, String, Object, Object>>> factory) {
		this.factory = factory;
	}

	public void setDestroy(IProcessor<Object, Object> destroy) {
		this.destroy = destroy;
	}

	public void setCache(Integer cache) {
		if (cache != null && cache >= 0) {
			size = cache;
			this.cache = new ConcurrentWeakIdentityHashMap<Object, Object>(false);
		}
	}

	public void setExecutor(ExecutorService executor) {
		this.executor = executor;
	}

	public void setTimeout(Long timeout) {
		this.timeout = timeout;
	}
	
	public void setMeta(IInputResource<Object, Object> meta) {
		this.meta = meta;
	}

	protected Boolean sync(Node[] flag) {
		if (flag == null || flag.length == 0) try {
			synchronized(this) {
				this.flag = null;
				if (startSignal.getCount() == 0) startSignal = new CountDownLatch(1);
			}
			if (timeout == null) startSignal.await();
			else if (timeout < 0) startSignal.await(-timeout, TimeUnit.SECONDS);
			else if (timeout > 0 && !startSignal.await(timeout, TimeUnit.SECONDS)) throw new PlatformException(SchemaError.CREATE_TIMEOUT);
			return null;
		} catch (Exception e) {
			throw new PlatformException(SchemaError.CREATE_TIMEOUT, e);
		} else if (flag[0].getName().equals(this.flag)) return true;
		else synchronized(this) {
			this.flag = flag[0].getName();			
			if (cache != null) cache = new ConcurrentWeakIdentityHashMap<Object, Object>(false);
			if (destroy != null) if (executor == null) destroy.process(null);
			else executor.execute(new Runnable(){
				@Override
				public void run() {
					destroy.process(null);
			}});
			startSignal.countDown();
			return false;
		}
	}

	protected Object getCache(Object[] key) {
		if (key == null || key.length == 0) return cache;
		else {
			Map<Object, Object> cache = this.cache;
			for (int i = 0; i < key.length - 1; i++) {
				Object c = cache.get(key[i]);
				if (c == null) synchronized (cache) {
					c = cache.get(key);
					if (c == null) {
						c = new ConcurrentWeakIdentityHashMap<Object, Object>(false);
						cache.put(key, c);
					}
				}
				cache = (Map<Object, Object>) c;
			}
			Object c = cache.get(key[key.length - 1]);
			if (c == null) synchronized (cache) {
				c = cache.get(key);
				if (c == null) {
					c = size == 0 ? CacheBuilder.newBuilder().build() : CacheBuilder.newBuilder().maximumSize(size).build();
					cache.put(key, c);
				}
			}
			return c;
		}
	}
	
	protected List<Node[]> get(Object ... paras) {//null: if timeout and not ready
		LinkedList<Node[]> ret = new LinkedList<Node[]>();
		if (cache == null) {
			Object[] newparas = Arrays.copyOf(paras, paras.length + 1);
			newparas[paras.length] = Constants.Flag;
			Iterator<Node[]> result = ((List<Node[]>) meta.find(newparas)).iterator();
			int i = 0;
			while (result.hasNext()) {
				if (paras.length > i) {
					Node[] e = result.next();
					ret.add(e == null ? new Node[0] : e);
				} else if (sync((Node[])result.next()) == null) return startSignal.getCount() == 0 ? get(paras) : null;
				i++;
			}
		} else {
			Cache<Object, Node[]> c = (Cache<Object, Node[]>) getCache(new Object[]{id});
			for (Object para : paras) {
				Node[] r = c.getIfPresent(para);
				if (r == null) break;
				else ret.add(r);
			}
			if (ret.size() < paras.length) {
				Object[] newparas = Arrays.copyOf(paras, paras.length + 1);
				newparas[paras.length] = Constants.Flag;
				Iterator<Node[]> result = ((List<Node[]>) meta.find(newparas)).iterator();
				ret = new LinkedList<Node[]>();
				int i = 0;
				while (result.hasNext()) {
					if (paras.length > i)  {
						Node[] r = result.next();
						if (r == null) r = new Node[0];
						c.put(paras[i], r);
						ret.add(r);
					} else if (sync((Node[])result.next()) == null) return startSignal.getCount() == 0 ? get(paras) : null;
					i++;
				}
			}
		}
		return ret;
	}
	
	@Override
	public Boolean process(Object para) {
		List<Node[]> ls = (List<Node[]>) meta.find(Constants.Flag);
		Node[] flag = ls == null || ls.size() == 0 ? null : ls.get(0);
		if (flag == null || flag.length == 0) synchronized(this) {
			this.flag = null;
			if (startSignal.getCount() == 0) startSignal = new CountDownLatch(1);
			return null;
		} else if (!flag[0].getName().equals(this.flag)) synchronized(this) {
			this.flag = flag[0].getName();
			if (cache != null) cache = new ConcurrentWeakIdentityHashMap<Object, Object>(false);
			if (destroy != null) destroy.process(null);
			startSignal.countDown();
			return false;
		}
		return true;
	}
	
	@Override
	public ITriProcessor<Object, String, Object, Object> operate(Object name, String type, Map<Object, Object> params) {
		ITriProcessor<Object, IBiProcessor<String, Object[], Object>, Map<Object, Object>, ITriProcessor<Object, String, Object, Object>> handler = factory.process(type);
		return handler == null ? null : handler.operate(name, this, params);
	}
	
	@Override
	public Object perform(String method, Object[] params) {
		switch (Operate.valueOf(method)) {
			case get: return params == null || params.length == 0 ? null : get(params);
			case getCache : return cache == null ? null : getCache(params == null || params.length == 0 ? null : params);	
			case sync: return sync((Node[])params);
			case process: return process(null);
			case operate: if (params == null || params.length < 2) return null;
			else return operate(params[0], (String) params[1], params.length > 2 && params[2] instanceof Map ? (Map<Object, Object>) params[2] : null);
		}
		return null;
	}
}
