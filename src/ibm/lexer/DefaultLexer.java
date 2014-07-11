package ibm.lexer;

public abstract class DefaultLexer
  implements Lexer
{
  protected int tokenStart;
  protected int tokenLength;

  protected Token token(TokenType type, int tStart, int tLength, int newStart, int newLength)
  {
    this.tokenStart = newStart;
    this.tokenLength = newLength;
    return new Token(type, tStart, tLength);
  }

  protected CharSequence getTokenSrring()
  {
    return yytext();
  }
}