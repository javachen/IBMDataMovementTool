package ibm.lexer;

import java.io.IOException;
import java.io.Reader;

public abstract interface Lexer
{
  public abstract void yyreset(Reader paramReader);

  public abstract Token yylex()
    throws IOException;

  public abstract char yycharat(int paramInt);

  public abstract int yylength();

  public abstract String yytext();
}