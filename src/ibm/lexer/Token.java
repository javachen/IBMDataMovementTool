package ibm.lexer;

import java.io.Serializable;

public class Token implements Serializable, Comparable<Token> {
	/**
	 * 2011-3-25
	 */
	private static final long serialVersionUID = 1L;
	public final TokenType type;
	public final int start;
	public final int length;
	public String text;
	public final byte pairValue;

	public Token(TokenType type, int start, int length) {
		this.type = type;
		this.start = start;
		this.length = length;
		this.pairValue = 0;
	}

	public Token(TokenType type, int start, int length, String text) {
		this.type = type;
		this.start = start;
		this.length = length;
		this.text = text;
		this.pairValue = 0;
	}

	public Token(TokenType type, int start, int length, String text,
			byte pairValue) {
		this.type = type;
		this.start = start;
		this.length = length;
		this.text = text;
		this.pairValue = pairValue;
	}

	public boolean equals(Object obj) {
		if ((obj instanceof Object)) {
			Token token = (Token) obj;
			return (this.start == token.start) && (this.length == token.length)
					&& (this.type.equals(token.type));
		}

		return false;
	}

	public int hashCode() {
		return this.start;
	}

	public String toString() {
		return String.format("%s (%d, %d) (%d)", new Object[] { this.type,
				Integer.valueOf(this.start), Integer.valueOf(this.length),
				Byte.valueOf(this.pairValue) });
	}

	public int compareTo(Token o) {
		Token t = (Token) o;
		if (this.start != t.start) {
			return this.start - t.start;
		}
		if (this.length != t.length) {
			return this.length - t.length;
		}

		return this.type.compareTo(t.type);
	}

	public int end() {
		return this.start + this.length;
	}

	public String getText(char[] doc) {
		String text = null;
		try {
			text = String.valueOf(doc, this.start, this.length);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return text;
	}

	public String getText() {
		return this.text;
	}
}