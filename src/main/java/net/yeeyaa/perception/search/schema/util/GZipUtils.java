package net.yeeyaa.perception.search.schema.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.yeeyaa.eight.IBiProcessor;
import net.yeeyaa.eight.IProcessor;
import net.yeeyaa.eight.PlatformException;
import net.yeeyaa.eight.core.util.BASE64Decoder;
import net.yeeyaa.eight.core.util.BASE64Encoder;
import net.yeeyaa.eight.core.util.TypeConvertor;
import net.yeeyaa.perception.search.schema.SchemaError;


public class GZipUtils implements IProcessor<Object, Object>, IBiProcessor<Object, Boolean, Object>{
	protected static BASE64Encoder encoder = new BASE64Encoder(3, 0);
	protected static BASE64Decoder decoder = new BASE64Decoder(3);	
	protected boolean compress;
	
    public void setCompress(boolean compress) {
		this.compress = compress;
	}

	public static String compress(String src) throws IOException {
        return encoder.encode(compress(src.getBytes("UTF-8")));
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
        return new String(decompress(decoder.decode(src)), "UTF-8");
    }

    public static byte[] decompress(byte[] compressedBytes) throws IOException {
        if (null != compressedBytes && compressedBytes.length > 0) {
            ByteArrayOutputStream out = null;
            ByteArrayInputStream in = null;
            GZIPInputStream gzip = null;
            try {
                out = new ByteArrayOutputStream();
                in = new ByteArrayInputStream(compressedBytes);
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
	
	public static void main(String[] args) {
		if (args != null && args.length > 0) try {
			String content = new String(TypeConvertor.urlToBytes(TypeConvertor.strToUrl(args[0]), 8196, -1L), "UTF-8");
			content = content.startsWith("[") || content.startsWith("{") ? compress(content) : decompress(content);
			if (args.length > 1) TypeConvertor.bytesToFile(content.getBytes("UTF-8"), args[1]);
			else System.out.println(content);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
