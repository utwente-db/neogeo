package nl.utwente.db.neogeo.preaggregate;

import java.util.Arrays;

public final class AggrKey {
    
        final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
        public static String bytesToHex(byte[] bytes) {
            char[] hexChars = new char[bytes.length * 2];
            for ( int j = 0; j < bytes.length; j++ ) {
                int v = bytes[j] & 0xFF;
                hexChars[j * 2] = hexArray[v >>> 4];
                hexChars[j * 2 + 1] = hexArray[v & 0x0F];
            }
            return new String(hexChars);
        }
        
        
    
        public static byte[] hexStringToByteArray(String s) {
            int len = s.length();
            byte[] data = new byte[len / 2];
            for (int i = 0; i < len; i += 2) {
                data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                     + Character.digit(s.charAt(i+1), 16));
            }
            return data;
        }
        
        public static AggrKey decodeByteKey (AggrKeyDescriptor kd, String keyHex) throws InvalidKeyException {
            byte[] arr = hexStringToByteArray(keyHex);
            return decodeByteKey(kd, arr);
        }
        
        public static AggrKey decodeByteKey (AggrKeyDescriptor kd, byte[] arr) throws InvalidKeyException {
            // can only accept KeyDescriptor of BYTE_STRING kind
            if (kd.kind() != AggrKeyDescriptor.KD_BYTE_STRING) {
                return null;
            }
            
            AggrKey ret = new AggrKey(kd);
            
            // verify length of key
            if (arr.length != kd.getTotalBytes()) {
                throw new InvalidKeyException("Number of bytes in ByteKey (" + arr.length + ") is not as expected (" + kd.getTotalBytes() + ")");
            }
            
            short idx=0;
            for(short i=0; i < kd.dimensions(); i++) {
                short level = -1;
                if (kd.levelBytes == 1) {
                    level = (short) arr[idx++];
                } else if (kd.levelBytes == 2) {
                    level = (short) (arr[idx++] << 8 | (arr[idx++] & 0xFF));
                }
                
                int index = -1;
                if (kd.dimBytes[i] == 1) {
                    index = (int) arr[idx++];
                } else if (kd.dimBytes[i] == 2) {
                    index = (int) (arr[idx++] << 8 | (arr[idx++] & 0xFF));
                } else if (kd.dimBytes[i] == 3) {
                    index = (int) (arr[idx++] << 16 | (arr[idx++] & 0xFF) << 8 | (arr[idx++] & 0xFF));
                } else if (kd.dimBytes[i] == 4) {
                    index = arr[idx++] << 24 | (arr[idx++] & 0xFF) << 16 | (arr[idx++] & 0xFF) << 8 | (arr[idx++] & 0xFF);
                }
                
                ret.setLevel(i, level);
                ret.setIndex(i, index);
            }
            
            return ret;
        }
        
        public static class InvalidKeyException extends Exception {
            public InvalidKeyException (String msg) {
                super(msg);
            }
        }
	
	private AggrKeyDescriptor kd;
	private int[]	data;
	
	public AggrKey(AggrKeyDescriptor kd) {
		this(kd,new int[kd.dimensions()*2]);	
	}
	
	public AggrKey(AggrKeyDescriptor kd, int data[]) {
		if ( (kd == null) || (data == null) ) {
			throw new NullPointerException();
		}
		this.kd	   = kd;
		this.data  = data;
	}
	
	public final void reset() {
		for(int i=0; i<data.length; i++) {
			data[i] = 0;
		}
	}
	
	public AggrKeyDescriptor kd() {
		return this.kd;
	}
	
	public AggrKey copy() {
		return new AggrKey(kd, data.clone());
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof AggrKey)) {
			return false;
		}
		return Arrays.equals(data, ((AggrKey) other).data);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(data);
	}
	
	/*
	 * 
	 * 
	 */
	
	public void setIndex(short dim, int i) {
		this.data[dim] = i;
	}
	
	public void setLevel(short dim, short l) {
		this.data[kd.dimensions()+dim] = (int)l;
	}
	
	public int getIndex(short dim) { // should maybe be called range
		return data[dim];	
	}
	
	public short getLevel(short dim) {
		return (short)this.data[kd.dimensions()+dim];
	}
	
	public boolean isSubindexed() {
		return kd.isSubindexed();
	}
	
	public long crossproductLongKey() {	
		long res = 0;
		// long nres = 0;
		
		for(short i=0; i<kd.dimensions(); i++) {	
			res = ( (res << kd.levelBits) + (long)getLevel(i) );
			res = ( (res << kd.dimBits[i]) + kd.axis[i].dimensionKeyValue(getIndex(i)) );
		}
		// System.out.println("# "+toString()+"="+res+"[nres="+nres+"]");
		return res;
	}
        
        
        
        
        
        public String byteKey () {            
            byte[] arr = new byte[kd.getTotalBytes()];
                        
            byte idx = 0;            
            for(short i=0; i < kd.dimensions(); i++) {
                short level = getLevel(i);
                int index = getIndex(i);
                
                // TODO: don't depend on bit shifting, simply use division/module
                
                if (kd.getLevelBytes() == 1) {
                    arr[idx++] = (byte)level;
                } else if (kd.getLevelBytes() == 2) {    
                    arr[idx++] = (byte)(level >> 8);
                    arr[idx++] = (byte)(level);
                }
                
                if (kd.dimBytes[i] == 4) {
                    arr[idx++] = (byte)(index >> 24);
                }
                
                if (kd.dimBytes[i] >= 3) {
                    arr[idx++] = (byte)(index >> 16);
                }
                
                if (kd.dimBytes[i] >= 2) {
                    arr[idx++] = (byte)(index >> 8);
                }
                
                arr[idx++] = (byte)(index);
            }
            
            return bytesToHex(arr);
        }
        
        
	
	public Object toKey() {
		switch (kd.kind()) {	
		 case AggrKeyDescriptor.KD_CROSSPRODUCT_LONG:
			 return new Long(crossproductLongKey());
		 case AggrKeyDescriptor.KD_BYTE_STRING:
			 return byteKey();
		 default:
			 throw new RuntimeException("UNEXPECTED");
		}
	}
	
	public String toString() {
		short i;
		StringBuilder sb;

		if (kd == null)
			throw new NullPointerException();
		sb = new StringBuilder();
		sb.append("AggrKey<kd=");
		sb.append(kd.toString());
		sb.append(",i=[");
		for (i = 0; i < kd.dimensions(); i++) {
			if (i > 0)
				sb.append(",");
			sb.append(getIndex(i));
		}
		sb.append("]");
		if (kd.isSubindexed()) {
			sb.append(",s=[");
			for (i = 0; i < kd.dimensions(); i++) {
				if (i > 0)
					sb.append(",");
				sb.append(getLevel(i));
			}
			sb.append("]");
		}
		sb.append(")>");
		return sb.toString();
	}
        
        
	
}
