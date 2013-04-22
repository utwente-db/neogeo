package nl.utwente.db.neogeo.utils.test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

public class PrintOutputStream extends OutputStream {
	
	private PrintStream printStream;

	public PrintOutputStream(PrintStream printStream) {
		this.printStream = printStream;
	}
	
	@Override
	public void write(int b) throws IOException {
		printStream.print((char)b);
	}
}