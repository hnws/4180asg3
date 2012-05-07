import java.io.*;
import java.util.*;
import java.security.*;
import com.rackspacecloud.client.cloudfiles.*;

public class Downloader {

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

	public static FilesClient downloader;

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

	public static void fileDown(String filepath) {
		try {
			byte[] FileMeta = downloader.getObject("meta", filepath);
			String MetaString = new String(FileMeta);
			System.out.println(MetaString);

			Scanner sc = new Scanner(MetaString);
			String nextSHA1;
			Integer val;

			while (sc.hasNext()) {
				nextSHA1 = sc.next();
				byte[] content = downloader.getObject("chunks", nextSHA1);
				String StrContent = new String(content);
				System.out.println(StrContent);
			}
			sc.close();
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

		downloader = new FilesClient("group10:group10", "lOnGjObpowS72", "http://10.10.10.1:8080/auth/v1.0");
		downloader.setConnectionTimeOut(10000);

		try {
			downloader.login();
			/*BEGIN	CLEAN ALL !!!!!	*/
			/*
			try {
				List<FilesObject> fos = downloader.listObjects("chunks");
				for (FilesObject fo:fos){
					downloader.deleteObject("chunks", fo.getName());
				}   
			} catch (Exception e) {
				e.printStackTrace();
			}   
			downloader.deleteContainer("chunks");
			try {
				List<FilesObject> fos = downloader.listObjects("meta");
				for (FilesObject fo:fos){
					downloader.deleteObject("meta", fo.getName());
				}   
			} catch (Exception e) {
				e.printStackTrace();
			}   
			downloader.deleteContainer("meta");
			*/
			/*END	CLEAN ALL !!!!! */
		} catch (Exception e) {
			e.printStackTrace();
		}

		while (!exit) {
			System.out.print("[cmd (down/exit)?] ");
			input = (scan.nextLine()).trim();
			if (input.length() < 4) {
				System.out.println("Invalid command!");
				continue;
			}
			cmd = input.substring(0, 4);
			if (cmd.equals("down")) {
				filepath = input.substring(5);
				fileDown(filepath);
			} else if (cmd.equals("exit")) {
				exit = true;
			} else {
				System.out.println("Invalid command!");
			}
		}
		System.out.println("Bye Bye!");
		scan.close();
	}
}
