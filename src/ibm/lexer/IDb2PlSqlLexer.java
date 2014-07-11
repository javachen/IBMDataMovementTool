package ibm.lexer;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

final class IDb2PlSqlLexer extends DefaultLexer {
	public static final int YYEOF = -1;
	private static final int ZZ_BUFFERSIZE = 16384;
	public static final int YYINITIAL = 0;
	private static final int[] ZZ_LEXSTATE = { 0, 1 };
	private static final String ZZ_CMAP_PACKED = "";
	private static final char[] ZZ_CMAP = zzUnpackCMap("");

	private static final int[] ZZ_ACTION = zzUnpackAction();
	private static final String ZZ_ACTION_PACKED_0 = "";
	private static final int[] ZZ_ROWMAP = zzUnpackRowMap();
	private static final String ZZ_ROWMAP_PACKED_0 = "";
	private static final int[] ZZ_TRANS = zzUnpackTrans();
	private static final String ZZ_TRANS_PACKED_0 = "";
	private static final int ZZ_UNKNOWN_ERROR = 0;
	private static final int ZZ_NO_MATCH = 1;
	private static final int ZZ_PUSHBACK_2BIG = 2;
	private static final String[] ZZ_ERROR_MSG = {
			"Unkown internal scanner error", "Error: could not match input",
			"Error: pushback value was too large" };

	private static final int[] ZZ_ATTRIBUTE = zzUnpackAttribute();
	private static final String ZZ_ATTRIBUTE_PACKED_0 = "";
	private Reader zzReader;
	private int zzState;
	private int zzLexicalState = 0;

	private char[] zzBuffer = new char[16384];
	private int zzMarkedPos;
	private int zzCurrentPos;
	private int zzStartRead;
	private int zzEndRead;
	private int yyline;
	private int yychar;
	private int yycolumn;
	private boolean zzAtBOL = true;
	private boolean zzAtEOF;
	private boolean zzEOFDone;

	private static int[] zzUnpackAction() {
		int[] result = new int['å'];
		int offset = 0;
		offset = zzUnpackAction("", offset, result);
		return result;
	}

	private static int zzUnpackAction(String packed, int offset, int[] result) {
		int i = 0;
		int j = offset;
		int l = packed.length();
		while (i < l) {
			int count = packed.charAt(i++);
			int value = packed.charAt(i++);
			do {
				result[(j++)] = value;
				count--;
			} while (count > 0);
		}
		return j;
	}

	private static int[] zzUnpackRowMap() {
		int[] result = new int['å'];
		int offset = 0;
		offset = zzUnpackRowMap("", offset, result);
		return result;
	}

	private static int zzUnpackRowMap(String packed, int offset, int[] result) {
		int i = 0;
		int j = offset;
		int l = packed.length();
		while (i < l) {
			int high = packed.charAt(i++) << '\020';
			result[(j++)] = (high | packed.charAt(i++));
		}
		return j;
	}

	private static int[] zzUnpackTrans() {
		int[] result = new int[9416];
		int offset = 0;
		offset = zzUnpackTrans("", offset, result);
		return result;
	}

	private static int zzUnpackTrans(String packed, int offset, int[] result) {
		int i = 0;
		int j = offset;
		int l = packed.length();
		while (i < l) {
			int count = packed.charAt(i++);
			int value = packed.charAt(i++);
			value--;
			do {
				result[(j++)] = value;
				count--;
			} while (count > 0);
		}
		return j;
	}

	private static int[] zzUnpackAttribute() {
		int[] result = new int['å'];
		int offset = 0;
		offset = zzUnpackAttribute("", offset, result);
		return result;
	}

	private static int zzUnpackAttribute(String packed, int offset, int[] result) {
		int i = 0;
		int j = offset;
		int l = packed.length();
		while (i < l) {
			int count = packed.charAt(i++);
			int value = packed.charAt(i++);
			do {
				result[(j++)] = value;
				count--;
			} while (count > 0);
		}
		return j;
	}

	public IDb2PlSqlLexer() {
	}

	private Token token(TokenType type) {
		return new Token(type, this.yychar, yylength(), yytext());
	}

	IDb2PlSqlLexer(Reader in) {
		this.zzReader = in;
	}

	IDb2PlSqlLexer(InputStream in) {
		this(new InputStreamReader(in));
	}

	private static char[] zzUnpackCMap(String packed) {
		char[] map = new char[65536];
		int i = 0;
		int j = 0;
		while (i < 1806) {
			int count = packed.charAt(i++);
			char value = packed.charAt(i++);
			do {
				map[(j++)] = value;
				count--;
			} while (count > 0);
		}
		return map;
	}

	private boolean zzRefill() throws IOException {
		if (this.zzStartRead > 0) {
			System.arraycopy(this.zzBuffer, this.zzStartRead, this.zzBuffer, 0,
					this.zzEndRead - this.zzStartRead);

			this.zzEndRead -= this.zzStartRead;
			this.zzCurrentPos -= this.zzStartRead;
			this.zzMarkedPos -= this.zzStartRead;
			this.zzStartRead = 0;
		}

		if (this.zzCurrentPos >= this.zzBuffer.length) {
			char[] newBuffer = new char[this.zzCurrentPos * 2];
			System.arraycopy(this.zzBuffer, 0, newBuffer, 0,
					this.zzBuffer.length);
			this.zzBuffer = newBuffer;
		}

		int numRead = this.zzReader.read(this.zzBuffer, this.zzEndRead,
				this.zzBuffer.length - this.zzEndRead);

		if (numRead > 0) {
			this.zzEndRead += numRead;
			return false;
		}

		if (numRead == 0) {
			int c = this.zzReader.read();
			if (c == -1) {
				return true;
			}
			this.zzBuffer[(this.zzEndRead++)] = (char) c;
			return false;
		}

		return true;
	}

	public final void yyclose() throws IOException {
		this.zzAtEOF = true;
		this.zzEndRead = this.zzStartRead;

		if (this.zzReader != null)
			this.zzReader.close();
	}

	public final void yyreset(Reader reader) {
		this.zzReader = reader;
		this.zzAtBOL = true;
		this.zzAtEOF = false;
		this.zzEOFDone = false;
		this.zzEndRead = (this.zzStartRead = 0);
		this.zzCurrentPos = (this.zzMarkedPos = 0);
		this.yyline = (this.yychar = this.yycolumn = 0);
		this.zzLexicalState = 0;
	}

	public final int yystate() {
		return this.zzLexicalState;
	}

	public final void yybegin(int newState) {
		this.zzLexicalState = newState;
	}

	public final String yytext() {
		return new String(this.zzBuffer, this.zzStartRead, this.zzMarkedPos
				- this.zzStartRead);
	}

	public final char yycharat(int pos) {
		return this.zzBuffer[(this.zzStartRead + pos)];
	}

	public final int yylength() {
		return this.zzMarkedPos - this.zzStartRead;
	}

	private void zzScanError(int errorCode) {
		String message;
		try {
			message = ZZ_ERROR_MSG[errorCode];
		} catch (ArrayIndexOutOfBoundsException e) {
			message = ZZ_ERROR_MSG[0];
		}

		throw new Error(message);
	}

	public void yypushback(int number) {
		if (number > yylength()) {
			zzScanError(2);
		}
		this.zzMarkedPos -= number;
	}

	public Token yylex() throws IOException {
		int zzEndReadL = this.zzEndRead;
		char[] zzBufferL = this.zzBuffer;
		char[] zzCMapL = ZZ_CMAP;

		int[] zzTransL = ZZ_TRANS;
		int[] zzRowMapL = ZZ_ROWMAP;
		int[] zzAttrL = ZZ_ATTRIBUTE;
		while (true) {
			int zzMarkedPosL = this.zzMarkedPos;

			this.yychar += zzMarkedPosL - this.zzStartRead;

			if (zzMarkedPosL > this.zzStartRead) {
				switch (zzBufferL[(zzMarkedPosL - 1)]) {
				case '\n':
				case '\013':
				case '\f':
				case '':
				case ' ':
				case ' ':
					this.zzAtBOL = true;
					break;
				case '\r':
					if (zzMarkedPosL < zzEndReadL) {
						this.zzAtBOL = (zzBufferL[zzMarkedPosL] != '\n');
					} else if (this.zzAtEOF) {
						this.zzAtBOL = false;
					} else {
						boolean eof = zzRefill();
						zzMarkedPosL = this.zzMarkedPos;
						zzEndReadL = this.zzEndRead;
						zzBufferL = this.zzBuffer;
						if (eof)
							this.zzAtBOL = false;
						else
							this.zzAtBOL = (zzBufferL[zzMarkedPosL] != '\n');
					}
					break;
				default:
					this.zzAtBOL = false;
				}
			}
			int zzAction = -1;

			int zzCurrentPosL = this.zzCurrentPos = this.zzStartRead = zzMarkedPosL;

			if (this.zzAtBOL)
				this.zzState = ZZ_LEXSTATE[(this.zzLexicalState + 1)];
			else
				this.zzState = ZZ_LEXSTATE[this.zzLexicalState];
			int zzInput;
			while (true) {
				if (zzCurrentPosL < zzEndReadL) {
					zzInput = zzBufferL[(zzCurrentPosL++)];
				} else {
					if (this.zzAtEOF) {
						zzInput = -1;
						break;
					}

					this.zzCurrentPos = zzCurrentPosL;
					this.zzMarkedPos = zzMarkedPosL;
					boolean eof = zzRefill();

					zzCurrentPosL = this.zzCurrentPos;
					zzMarkedPosL = this.zzMarkedPos;
					zzBufferL = this.zzBuffer;
					zzEndReadL = this.zzEndRead;
					if (eof) {
						zzInput = -1;
						break;
					}

					zzInput = zzBufferL[(zzCurrentPosL++)];
				}

				int zzNext = zzTransL[(zzRowMapL[this.zzState] + zzCMapL[zzInput])];
				if (zzNext == -1)
					break;
				this.zzState = zzNext;

				int zzAttributes = zzAttrL[this.zzState];
				if ((zzAttributes & 0x1) == 1) {
					zzAction = this.zzState;
					zzMarkedPosL = zzCurrentPosL;
					if ((zzAttributes & 0x8) == 8) {
						break;
					}
				}
			}

			this.zzMarkedPos = zzMarkedPosL;

			switch (zzAction < 0 ? zzAction : ZZ_ACTION[zzAction]) {
			case 10:
				return token(TokenType.KEYWORD);
			case 13:
				break;
			case 12:
				return token(TokenType.IDB2WORDS);
			case 14:
				break;
			case 2:
				return token(TokenType.LINE);
			case 15:
				break;
			case 9:
				return token(TokenType.CHARLITERAL);
			case 16:
				break;
			case 7:
				return token(TokenType.SQLTERMINATOR);
			case 17:
				break;
			case 4:
				return token(TokenType.OPERATOR);
			case 18:
				break;
			case 5:
				return token(TokenType.IDENTIFIER);
			case 19:
				break;
			case 11:
				return token(TokenType.STRING);
			case 20:
				break;
			case 8:
				return token(TokenType.COMMENT);
			case 21:
				break;
			case 3:
				return token(TokenType.WHITESPACE);
			case 22:
				break;
			case 6:
				return token(TokenType.NUMBER);
			case 23:
				break;
			case 1:
			case 24:
				break;
			default:
				if ((zzInput == -1) && (this.zzStartRead == this.zzCurrentPos)) {
					this.zzAtEOF = true;

					return null;
				}

				zzScanError(1);
			}
		}
	}
}