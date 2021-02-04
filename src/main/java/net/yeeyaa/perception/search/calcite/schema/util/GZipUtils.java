package net.yeeyaa.perception.search.calcite.schema.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.yeeyaa.eight.IBiProcessor;
import net.yeeyaa.eight.IProcessor;
import net.yeeyaa.eight.PlatformException;
import net.yeeyaa.perception.search.calcite.schema.SchemaError;

import org.apache.commons.codec.binary.Base64;


public class GZipUtils implements IProcessor<Object, Object>, IBiProcessor<Object, Boolean, Object>{
	protected boolean compress;
	
    public void setCompress(boolean compress) {
		this.compress = compress;
	}

	public static String compress(String src) throws IOException {
        return Base64.encodeBase64String(compress(src.getBytes("UTF-8")));
    }

    public static byte[] compress(byte[] bytes) throws IOException {
        if (null != bytes && bytes.length > 0) {
            ByteArrayOutputStream out = null;
            GZIPOutputStream gzip = null;
            try {
                out = new ByteArrayOutputStream();
                gzip = new GZIPOutputStream(out);
                gzip.write(bytes);
            } finally {
                closeQuiet(out);
                closeQuiet(gzip);
            }
            return out.toByteArray();
        } else {
            return new byte[0];
        }
    }

    public static String decompress(String src) throws IOException {
        return new String(decompress(Base64.decodeBase64(src)), "UTF-8");
    }

    public static byte[] decompress(byte[] compressedBytes) throws IOException {
        if (null != compressedBytes && compressedBytes.length > 0) {
            ByteArrayOutputStream out = null;
            ByteArrayInputStream in = null;
            GZIPInputStream gzip = null;
            try {
                out = new ByteArrayOutputStream();
                in = new ByteArrayInputStream(Base64.decodeBase64(compressedBytes));
                gzip = new GZIPInputStream(in);
                byte[] buffer = new byte[256];
                int n;
                while((n = gzip.read(buffer)) >= 0) out.write(buffer, 0, n);
            } finally {
                closeQuiet(out);
                closeQuiet(in);
                closeQuiet(gzip);
            }
            return out.toByteArray();
        } else {
            return new byte[0];
        }
    }

    private static void closeQuiet(Closeable c) {
        if (c != null) try {
            c.close();
        } catch (IOException e) {}
    }

	@Override
	public Object perform(Object object, Boolean compress) {
		if (object != null) try {
			if (Boolean.TRUE.equals(compress)) if (object instanceof byte[]) return compress((byte[])object);
			else return compress(object.toString());
			else if (object instanceof byte[]) return decompress((byte[])object);
			else return decompress(object.toString());
		} catch (Exception e) {
			throw new PlatformException(SchemaError.ERROR_PARAMETERS, "compress fail.", e);
		} else return null;
	}

	@Override
	public Object process(Object object) {
		return perform(object, compress);
	}
}
