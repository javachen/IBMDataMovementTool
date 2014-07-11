package ibm.lexer;

import java.io.BufferedReader;
import java.io.CharArrayReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {
	char[] charsBuffer;
	private int bufferLength = 0;
	Lexer lexer;
	List<Token> tokens;

	public Parser(Lexer lexer) {
		this.lexer = lexer;
	}

	public char[] getCharsBuffer() {
		return this.charsBuffer;
	}

	public String getText(int start) {
		return String.valueOf(this.charsBuffer, start, this.charsBuffer.length
				- start);
	}

	public String getText(int start, int length) {
		return String.valueOf(this.charsBuffer, start, length);
	}

	public void parse(String buffer) {
		this.charsBuffer = buffer.toCharArray();
		CharArrayReader reader = new CharArrayReader(this.charsBuffer);
		parse(reader);
	}

	public void parse(Reader reader) {
		if ((this.lexer == null) || (reader == null)) {
			this.tokens = null;
			return;
		}
		List toks = new ArrayList(this.bufferLength / 10);
		long ts = System.nanoTime();
		int len = this.bufferLength;
		try {
			this.lexer.yyreset(reader);
			Token token;
			while ((token = this.lexer.yylex()) != null) {
				toks.add(token);
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			this.tokens = toks;
		}
	}

	public void replaceToken(Token token, String replacement) {
	}

	public Matcher getMatcher(Pattern pattern) {
		return getMatcher(pattern, 0, this.bufferLength);
	}

	public Matcher getMatcher(Pattern pattern, int start) {
		return getMatcher(pattern, start, this.bufferLength - start);
	}

	public Matcher getMatcher(Pattern pattern, int start, int length) {
		Matcher matcher = null;
		if (this.bufferLength == 0) {
			return null;
		}
		matcher = pattern.matcher(getText(start, length));
		return matcher;
	}

	public Token getTokenAt(int pos) {
		if ((this.tokens == null) || (this.tokens.isEmpty())
				|| (pos > this.bufferLength)) {
			return null;
		}
		Token tok = null;
		Token tKey = new Token(TokenType.DEFAULT, pos, 1);

		int ndx = Collections.binarySearch(this.tokens, tKey);
		if (ndx < 0) {
			ndx = -ndx - 1 - 1 < 0 ? 0 : -ndx - 1 - 1;
			Token t = (Token) this.tokens.get(ndx);
			if ((t.start <= pos) && (pos <= t.end())) {
				tok = t;
			}
		} else {
			tok = (Token) this.tokens.get(ndx);
		}
		return tok;
	}

	public Token getPairFor(Token t) {
		if ((t == null) || (t.pairValue == 0)) {
			return null;
		}
		Token p = null;
		int ndx = this.tokens.indexOf(t);

		int w = t.pairValue;
		int direction = t.pairValue > 0 ? 1 : -1;
		boolean done = false;
		int v = Math.abs(t.pairValue);
		while (!done) {
			ndx += direction;
			if ((ndx < 0) || (ndx >= this.tokens.size())) {
				break;
			}
			Token current = (Token) this.tokens.get(ndx);
			if (Math.abs(current.pairValue) == v) {
				w += current.pairValue;
				if (w == 0) {
					p = current;
					done = true;
				}
			}
		}
		return p;
	}

	public String getBufferContents(String buffer) {
		if ((buffer == null) || (buffer.length() == 0)) {
			this.bufferLength = 0;
			return null;
		}

		this.bufferLength = buffer.length();
		return buffer;
	}

	public void getUncommentedText(int aStart, int anEnd) {
		StringBuilder result = new StringBuilder();
		Iterator iter = getTokens(aStart, anEnd);
		while (iter.hasNext()) {
			Token t = (Token) iter.next();
			if ((TokenType.COMMENT != t.type) && (TokenType.COMMENT2 != t.type)) {
				result.append(t.getText(this.charsBuffer));
			}
		}
	}

	public String getFileContents(String fileName) {
		StringBuilder sb = new StringBuilder();
		try {
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			char[] buffer = new char[30000];
			int n = this.bufferLength = 0;
			while (n >= 0) {
				n = br.read(buffer, 0, buffer.length);
				if (n <= 0)
					continue;
				sb.append(buffer, 0, n);
				this.bufferLength += n;
			}

			if (br != null)
				br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	public TokenIterator getTokens() {
		return getTokens(0, this.charsBuffer.length);
	}

	public TokenIterator getTokens(int start, int end) {
		return new TokenIterator(start, end);
	}

	class TokenIterator implements ListIterator<Token> {
		int start;
		int end;
		int ndx = 0;

		private TokenIterator(int start, int end) {
			this.start = start;
			this.end = end;
			Token token;
			if ((Parser.this.tokens != null) && (!Parser.this.tokens.isEmpty())) {
				token = new Token(TokenType.COMMENT, start, end - start);
				this.ndx = Collections.binarySearch(Parser.this.tokens, token);

				if (this.ndx < 0) {
					this.ndx = (-this.ndx - 1 - 1 < 0 ? 0 : -this.ndx - 1 - 1);
					Token t = (Token) Parser.this.tokens.get(this.ndx);

					if (t.end() <= start) {
						this.ndx += 1;
					}
				}
			}
		}

		public boolean hasNext() {
			if (Parser.this.tokens == null) {
				return false;
			}
			if (this.ndx >= Parser.this.tokens.size()) {
				return false;
			}
			Token t = (Token) Parser.this.tokens.get(this.ndx);

			return t.start < this.end;
		}

		public Token next() {
			return (Token) Parser.this.tokens.get(this.ndx++);
		}

		public Token nextToken() {
			this.ndx += 1;
			while ((((Token) Parser.this.tokens.get(this.ndx)).type == TokenType.WHITESPACE)
					|| (((Token) Parser.this.tokens.get(this.ndx)).type == TokenType.LINE)) {
				this.ndx += 1;
			}
			return (Token) Parser.this.tokens.get(this.ndx);
		}

		public Token next(TokenType type) {
			this.ndx += 1;
			while (((Token) Parser.this.tokens.get(this.ndx)).type != type) {
				this.ndx += 1;
			}
			return (Token) Parser.this.tokens.get(this.ndx);
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}

		public boolean hasPrevious() {
			if (Parser.this.tokens == null) {
				return false;
			}
			if (this.ndx <= 0) {
				return false;
			}
			Token t = (Token) Parser.this.tokens.get(this.ndx);

			return t.end() > this.start;
		}

		public Token previous() {
			return (Token) Parser.this.tokens.get(this.ndx--);
		}

		public int nextIndex() {
			return this.ndx + 1;
		}

		public int previousIndex() {
			return this.ndx - 1;
		}

		public void set(Token e) {
			throw new UnsupportedOperationException();
		}

		public void add(Token e) {
			throw new UnsupportedOperationException();
		}
	}
}