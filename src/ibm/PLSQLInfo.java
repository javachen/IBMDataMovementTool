package ibm;

public class PLSQLInfo
{
  public String codeStatus;
  public String type;
  public String schema;
  public String object;
  public String plSQLCode;
  public String lineNumber;

  public PLSQLInfo(String codeStatus, String type, String schema, String object, String lineNumber, String plSQLCode)
  {
    this.codeStatus = codeStatus;
    this.type = type;
    this.schema = schema;
    this.object = object;
    this.plSQLCode = plSQLCode;
    this.lineNumber = lineNumber;
  }

  public String toString()
  {
    return this.codeStatus + this.object;
  }
}