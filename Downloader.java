import java.io.*;
import java.util.*;
import java.security.*;
import com.rackspacecloud.client.cloudfiles.*;
import org.apache.http.HttpException;

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
		byte[] FileMeta = new byte[1];
		try {
			FileMeta = downloader.getObject("meta", filepath);
		} catch (FilesNotFoundException e) {
			System.out.println("File not found on cloud");
			return;
		} catch (IOException e) {
			System.out.println("There was an IO error doing network communication");
		} catch (HttpException e) {
			System.out.println("There was an error with the http protocol");
		}
		String MetaString = new String(FileMeta);
		Scanner sc = new Scanner(MetaString);
		String nextSHA1;
		Integer val;
		FileOutputStream fos = null;
		System.out.print("Where do you want to save? ");
		Scanner scin = new Scanner(System.in);
		String savepath = scin.nextLine();
		try {
			fos = new FileOutputStream(savepath);
		} catch (FileNotFoundException e){
			System.out.println("Cannot write to file " + savepath);
			return;
		}

		while (sc.hasNext()) {
			nextSHA1 = sc.next();
			byte[] content = new byte[1];
			try {
				content = downloader.getObject("chunks", nextSHA1);
				fos.write(content);
			} catch (FilesNotFoundException e) {
				System.out.println("chunk " + nextSHA1 + " missing on cloud");
			} catch (IOException e) {
				System.out.println("There was an IO error doing network communication");
			} catch (HttpException e) {
				System.out.println("There was an error with the http protocol");
			}
		}
		sc.close();
		try {
			fos.close();
		} catch (IOException e) {
			System.out.println("Please be kind");
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
		boolean SuccessLogin = false;
		try {
			SuccessLogin = downloader.login();
		} catch (IOException e) {
			System.out.println("There was an IO error doing network communication");
		} catch (HttpException e) {
			System.out.println("There was an error with the http protocol");
		}

		if (!SuccessLogin) {
			System.out.println("username/password is not correct");
			return;
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
