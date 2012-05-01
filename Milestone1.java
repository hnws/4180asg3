import java.io.File;
import java.io.FileInputStream;
import java.util.Queue;
import java.util.LinkedList;

public class Milestone1 {

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

	public static void chunkFile(File file, int m, long d, long q, long maxSize, long mask) {
		init(d, m, q);

		try {
			FileInputStream fis = new FileInputStream(file);
			int n, head = 0, tail = 0, bval; 
			long rfp = 0, offset = 0, lastPos = 0;
			int[] byteQ = new int[m];
			byte[] buf = new byte[1<<20];
			
			System.out.println(offset);
			int cnt = 0;
			n = fis.read(buf, 0, m);
			if (n > 0) {
				offset += n;
				for (int i = 0; i < n; ++i) {
					bval = ((int) buf[i]) & 0xFF;
					byteQ[tail] = bval;
					tail = (tail + 1) % m;
					rfp = ((rfp * d ) & q) + bval;
				}
				while ((n = fis.read(buf, 0, 1<<20)) > 0) {
					for (int i = 0; i < n; ++i) {
						if ((offset - lastPos >= m) && ((rfp & mask) == 0))  {
							System.out.println(offset);
							lastPos = offset;
						} else if (offset - lastPos == maxSize) {
							System.out.println(offset);
							lastPos = offset;
						}
						bval = ((int) buf[i]) & 0xFF;
						rfp = (((rfp * d) & q) + Q - modQ[byteQ[head]] + bval) & q;
						head = (head + 1) % m;
						byteQ[tail] = bval;
						tail = (tail + 1) % m;
						offset++;
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Usage: java chunking filepath");
		}
		try {
			File inputFile = new File(args[0]);
			if (inputFile.exists() && inputFile.isFile() && inputFile.canRead()) {
				if (inputFile.length() < sizeBound) {
					chunkFile(inputFile, smallM, d, q, smallMax, smallMask);
				} else {
					chunkFile(inputFile, largeM, d, q, largeMax, largeMask);
				}
			} else {
				System.out.println("Filepath is invalid or file cannot be read");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
