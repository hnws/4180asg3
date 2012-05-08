import java.io.*;
import java.util.*;
import java.security.*;
import com.rackspacecloud.client.cloudfiles.*;

public class Milestone3 {

	public static final long sizeBound = (1<<20);
	public static final int  smallM = 4;
	public static final long smallMax = 1024;
	public static final long smallMask = 0xF;
	public static final int  largeM = 1024;
	public static final long largeMax = (1<<20);
	public static final long largeMask = 0xFFF;
	public static final long  d = 257;
	public static final long q = 0xFFFFFFFFL;	// mod q (1<<32)
	public static final long Q = 0x100000000L;	// mod q (1<<32)
	public static long[] modQ = new long[256];

	public static long totalChunk = 0;
	public static long uniqueChunk = 0;
	public static long duplicatedChunk = 0;
	public static long byteWith = 0;
	public static long byteWithout = 0;

	public static FilesClient uploader;

	public static void init(long d, int m, long q) {
		long dm = 1;
		while (m > 0) {
			if ((m & 1) == 1) 
				dm = (dm * d) & q;
			m = m / 2;
			d = (d * d) & q;
		}
		for (int i = 0; i < 256; i++)
			modQ[i] = (i * dm) & q;
	}

	public static String bytesToHex(byte[] b) {
		char hexDigit[] = {'0', '1', '2', '3', '4', '5', '6', '7',
			'8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
		StringBuffer buf = new StringBuffer();
		for (int j=0; j < b.length; j++) {
			buf.append(hexDigit[(b[j] >> 4) & 0x0f]);
			buf.append(hexDigit[b[j] & 0x0f]);
		}
		return buf.toString();
	}

	public static Integer one = new Integer(1);

	@SuppressWarnings("unchecked")
	public static void manageChunk(byte[] chunk, int len, StringBuffer metaBuf) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA1");
			md.update(Arrays.copyOfRange(chunk, 0, len));

			byte[] sha1out = md.digest();
			String sha1hex = bytesToHex(sha1out);

			//	System.out.println("SHA1HEX = "+sha1hex);
			metaBuf.append(sha1hex);
			metaBuf.append('\n');

			Integer val = chunkIndex.get(sha1hex);
			if (val == null) {
				//Unique chunk!
				Map<String, String> mp = new HashMap();
				System.out.print("Writing " + len +" bytes to cloud...");
				uploader.storeObject("chunks", Arrays.copyOfRange(chunk, 0, len), "", sha1hex, mp);
				System.out.println("Done");
				chunkIndex.put(sha1hex, one);
				chunkSizeIndex.put(sha1hex, new Integer(len));
				uniqueChunk++;
				byteWith += len;
			} else {
				//Duplicated chunk! Do not upload
				chunkIndex.put(sha1hex, new Integer(val.intValue()+1));
				duplicatedChunk++;
			}	
			totalChunk ++;
			byteWithout += len;

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public static void chunkFile(File file, int m, long d, long q, long maxSize, long mask) {

		init(d, m, q);

		try {
			FileInputStream fis = new FileInputStream(file);
			StringBuffer metaBuf = new StringBuffer();
			// FileOutputStream fos = new FileOutputStream(file.getAbsolutePath()+".meta");

			int n, head = 0, tail = 0, bval, chunkLen = 0; 
			long rfp = 0, offset = 0, lastPos = 0;
			int[] byteQ = new int[m];
			byte[] buf = new byte[1<<20];
			byte[] chunk = new byte[(1<<20)+5];

			//		System.out.println(offset);
			if ((n = fis.read(buf, 0, m)) > 0) {
				offset += n;
				for (int i = 0; i < n; ++i) {
					bval = ((int) buf[i]) & 0xFF;
					byteQ[tail] = bval;
					tail = (tail + 1) % m;
					rfp = ((rfp * d ) & q) + bval;
					chunk[chunkLen++] = buf[i];
				}
				while ((n = fis.read(buf, 0, 1<<20)) > 0) {
					for (int i = 0; i < n; ++i) {
						if ((offset - lastPos >= m) && ((rfp & mask) == 0))  {
							manageChunk(chunk, chunkLen, metaBuf);
							chunkLen = 0;
							//						System.out.println(offset);
							lastPos = offset;
						} else if (offset - lastPos == maxSize) {
							manageChunk(chunk, chunkLen, metaBuf);
							chunkLen = 0;
							//						System.out.println(offset);
							lastPos = offset;
						}
						bval = ((int) buf[i]) & 0xFF;
						chunk[chunkLen++] = buf[i];
						rfp = (((rfp * d) & q) + Q - modQ[byteQ[head]] + bval) & q;
						head = (head + 1) % m;
						byteQ[tail] = bval;
						tail = (tail + 1) % m;
						offset++;
					}
				}
				if (chunkLen > 0) {
					manageChunk(chunk, chunkLen, metaBuf);
				}
			}

			// fos.write(metaBuf.toString().getBytes());
			// fos.close();
			Map<String, String> mp = new HashMap();
			uploader.storeObject("meta", metaBuf.toString().getBytes(), "", file.getAbsolutePath(), mp);
			fis.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void fileAdd(String filepath) {
		try {
			File inputFile = new File(filepath);
			if (inputFile.exists() && inputFile.isFile() && inputFile.canRead()) {
				System.out.println("Start processing......");
				if (inputFile.length() < sizeBound) {
					chunkFile(inputFile, smallM, d, q, smallMax, smallMask);
				} else {
					chunkFile(inputFile, largeM, d, q, largeMax, largeMask);
				}
				System.out.println("Done!");
				System.out.println("- Total chunks = " + totalChunk);
				System.out.println("- No. of unique chunks = " + uniqueChunk);
				System.out.println("- No. of duplicated chunks = " + duplicatedChunk);
				System.out.println("- No. of Bytes with deduplication = " + byteWith);
				System.out.println("- No. of Bytes without deduplication = " + byteWithout);

			} else {
				System.out.println("Filepath is invalid or file cannot be read");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void fileDel(String filepath) {
		try {
			//TODO: in milestone3, get the inputstream from swift instead of local file
			// File inputFile = new File(filepath+".meta");
			// if (inputFile.exists() && inputFile.isFile() && inputFile.canRead()) {
			//	System.out.print("Start processing......");

			//	FileInputStream fis = new FileInputStream(inputFile);

			byte[] FileMeta = uploader.getObject("meta", filepath);
			String MetaString = new String(FileMeta);
			System.out.println(MetaString);

			Scanner sc = new Scanner(MetaString);
			String nextSHA1;
			Integer val;

			while (sc.hasNext()) {
				nextSHA1 = sc.next();
				val = chunkIndex.get(nextSHA1);
				if (val == null) {
					System.out.println("Error: No such chunk!");
				} else 
					if (val.intValue() == 1) {
						uploader.deleteObject("chunks", nextSHA1);
						chunkIndex.remove(nextSHA1);
						Integer sval = chunkSizeIndex.remove(nextSHA1);
						uniqueChunk--;
						byteWith -= sval.intValue();
						byteWithout -= sval.intValue();
					} else {
						chunkIndex.put(nextSHA1, new Integer(val.intValue()-1));
						Integer sval = chunkSizeIndex.get(nextSHA1);
						duplicatedChunk --;
						byteWithout -= sval.intValue();
					}
				totalChunk --;
			}
			sc.close();
			//				fis.close();

			//				if (!inputFile.delete()) {
			//					System.out.println("Error: Cannot delete file: "+inputFile.getAbsolutePath());
			//				}

			System.out.println("Done!");
			uploader.deleteObject("meta", filepath);
			System.out.println("- Total chunks = " + totalChunk);
			System.out.println("- No. of unique chunks = " + uniqueChunk);
			System.out.println("- No. of duplicated chunks = " + duplicatedChunk);
			System.out.println("- No. of Bytes with deduplication = " + byteWith);
			System.out.println("- No. of Bytes without deduplication = " + byteWithout);

			//			} else {
			//				System.out.println("Filepath is invalid or file has not been uploaded");
			//			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static HashMap<String, Integer> chunkIndex = null;
	public static HashMap<String, Integer> chunkSizeIndex = null;

	public static void main(String[] args) {
		boolean exit = false;
		String input, cmd, filepath;
		Scanner scan = new Scanner(System.in);

		chunkIndex = new HashMap<String, Integer>();
		chunkSizeIndex = new HashMap<String, Integer>();

		uploader = new FilesClient("group10:group10", "lOnGjObpowS72", "http://10.10.10.1:8080/auth/v1.0");
		uploader.setConnectionTimeOut(10000);

		try {
			uploader.login();
			if (!uploader.containerExists("chunks")) 
				uploader.createContainer("chunks");
			else
				System.out.println("chunks is not clean, continue still");
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			if (!uploader.containerExists("meta")) 
				uploader.createContainer("meta");
			else
				System.out.println("meta in not clean, continue still");
		} catch (Exception e) {
			e.printStackTrace();
		}
		while (!exit) {
			System.out.print("[cmd (add/del/exit)?] ");
			input = (scan.nextLine()).trim();
			if (input.length() < 4) {
				System.out.println("Invalid command!");
				continue;
			}
			cmd = input.substring(0, 4);
			if (cmd.equals("add ")) {
				filepath = input.substring(4);
				fileAdd(filepath);
			} else 
				if (cmd.equals("del ")) {
					filepath = input.substring(4);
					fileDel(filepath);
				} else
					if (cmd.equals("exit")) {
						exit = true;
					} else {
						System.out.println("Invalid command!");
					}
		}
		System.out.println("Bye Bye!");
		scan.close();
	}
}
