package ibm;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.swing.JEditorPane;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

import com.ibm.db2.jcc.DB2Diagnosable;
import com.ibm.db2.jcc.DB2Sqlca;

public class RunDeployObjects
  implements Runnable
{
  private JEditorPane topArea;
  private IBMExtractConfig cfg;
  private boolean runFullTree = true;
  private int curIndex = 0; private int failedCount = 0; private int successCount = 0; private int discardCount = 0; private int existsCount = 0;
  private JTree tree;
  private Object[][] tabData;
  private Connection mainConn = null;
  private Properties deployedObjectsList;
  private String outputDirectory;
  private String DEPLOYED_OBJECT_FILE = "DeployedObjects.properties";
  public String sqlCode = ""; public String sqlMessage = ""; public String failedLineNumber = "";
  public String sqlTerminator;

  public RunDeployObjects(String sqlTerminator, String outputDirectory, JTree tree, boolean runFullTree, JEditorPane topArea)
  {
    this.sqlTerminator = sqlTerminator;
    this.runFullTree = runFullTree;
    this.outputDirectory = outputDirectory;
    this.deployedObjectsList = new Properties();
    this.tree = tree;
    this.topArea = topArea;
    this.cfg = new IBMExtractConfig();
    this.cfg.loadConfigFile();
    this.cfg.getParamValues();
    this.mainConn = IBMExtractUtilities.OpenConnection(this.cfg.getDstVendor(), this.cfg.getDstServer(), this.cfg.getDstPort(), this.cfg.getDstDBName(), this.cfg.getDstUid(), this.cfg.getDstPwd());

    this.sqlMessage = IBMExtractUtilities.Message;
  }

  public Object[][] getTabData()
  {
    return this.tabData;
  }

  public void setTabData(Object[][] tabData)
  {
    this.tabData = tabData;
  }

  private void deployObject(String type, String schema, String objectName, String sql)
  {
    String sqlerrmc = "";
    String value = ""; String key = type + ":" + schema + ":" + objectName;
    int lineNumber = -1;
    this.sqlMessage = (this.sqlCode = this.failedLineNumber = "");
    Statement statement = null;
    try
    {
      statement = this.mainConn.createStatement();
      try
      {
        statement.execute("SET CURRENT SCHEMA = '" + schema + "'");
        statement.execute("SET CURRENT FUNCTION PATH = SYSIBM,SYSFUN,SYSPROC,SYSCAT,SYSIBMADM," + schema);
      } catch (Exception ex) {
      }
      statement.execute(sql);
      value = "1";
      this.mainConn.commit();
      if (statement != null)
        statement.close();
    }
    catch (SQLException e) {
      if ((e instanceof DB2Diagnosable))
      {
        DB2Sqlca sqlca = ((DB2Diagnosable)e).getSqlca();
        if (sqlca != null)
        {
          lineNumber = sqlca.getSqlErrd()[2];
          sqlerrmc = sqlca.getSqlErrmc();
        }
      }
      if ((e.getErrorCode() == -601) || (e.getErrorCode() == -624))
      {
        this.sqlCode = "0";
        value = "1";
      }
      else {
        this.sqlCode = ("" + e.getErrorCode());
        value = "0";
        this.failedLineNumber = ("" + lineNumber);
        this.sqlMessage = IBMExtractUtilities.getSQLMessage(this.mainConn, this.sqlCode, sqlerrmc);
      }
    }
    this.deployedObjectsList.setProperty(key, value);
  }

  private Object[][] walk(Object[][] tabData, TreeModel model, Object o)
  {
    int cc = model.getChildCount(o);
    for (int i = 0; i < cc; i++)
    {
      DefaultMutableTreeNode child = (DefaultMutableTreeNode)model.getChild(o, i);
      if (child == null) return tabData;

      Object nodeInfo = child.getUserObject();
      if (model.isLeaf(child))
      {
        PLSQLInfo plsql = (PLSQLInfo)nodeInfo;
        if (plsql.codeStatus.equals("3"))
        {
          this.discardCount += 1;
          tabData[this.curIndex][0] = plsql.type;
          tabData[this.curIndex][1] = plsql.schema;
          tabData[this.curIndex][2] = plsql.object;
          tabData[this.curIndex][3] = plsql.codeStatus;
          tabData[this.curIndex][4] = "";
          tabData[this.curIndex][5] = "";
          tabData[this.curIndex][6] = "Object was not chosen to be deployed";
        }
        else
        {
          deployObject(plsql.type, plsql.schema, plsql.object, plsql.plSQLCode);
          tabData[this.curIndex][0] = plsql.type;
          tabData[this.curIndex][1] = plsql.schema;
          tabData[this.curIndex][2] = plsql.object;
          if (this.sqlCode.equals(""))
          {
            tabData[this.curIndex][3] = "1";
            plsql.codeStatus = "1";
            tabData[this.curIndex][4] = this.sqlCode;
            tabData[this.curIndex][5] = "";
            tabData[this.curIndex][6] = "Deployed";
            this.successCount += 1;
            IBMExtractUtilities.log(plsql.type + "->" + plsql.schema + "." + plsql.object + " deployed successfully.");
          }
          else if (this.sqlCode.equals("0"))
          {
            tabData[this.curIndex][3] = "2";
            plsql.codeStatus = "2";
            tabData[this.curIndex][4] = this.sqlCode;
            tabData[this.curIndex][5] = "";
            tabData[this.curIndex][6] = "Already exists";
            this.existsCount += 1;
            IBMExtractUtilities.log(plsql.type + "->" + plsql.schema + "." + plsql.object + " already exists.");
          }
          else
          {
            tabData[this.curIndex][3] = "0";
            plsql.codeStatus = "0";
            plsql.lineNumber = this.failedLineNumber;
            tabData[this.curIndex][4] = this.sqlCode;
            tabData[this.curIndex][5] = this.failedLineNumber;
            tabData[this.curIndex][6] = this.sqlMessage;
            this.failedCount += 1;
            IBMExtractUtilities.log(plsql.type + "->" + plsql.schema + "." + plsql.object + " deployment failed ");
          }
        }
        child.setUserObject(plsql);
        ((DefaultTreeModel)this.tree.getModel()).nodeStructureChanged(child);
        this.tree.scrollPathToVisible(new TreePath(child.getPath()));
        this.curIndex += 1;
      }
      else
      {
        tabData = walk(tabData, model, child);
      }
    }
    return tabData;
  }

  private int leafWalk(int count, TreeModel model, Object o)
  {
    int cc = model.getChildCount(o);
    for (int i = 0; i < cc; i++)
    {
      Object child = model.getChild(o, i);
      if (model.isLeaf(child))
      {
        count++;
      }
      else
      {
        count = leafWalk(count, model, child);
      }
    }
    return count;
  }

  private int getTotalObjects(JTree tree)
  {
    int count = 0;
    TreeModel model = tree.getModel();
    if (model != null)
    {
      Object root = model.getRoot();
      count = leafWalk(count, model, root);
    }
    return count;
  }

  public void run()
  {
    if (this.runFullTree)
      runFull();
    else
      runSelected();
  }

  public void runSelected()
  {
    if (this.mainConn == null)
    {
      this.tabData = new Object[1][7];
      this.tabData[this.curIndex][0] = "Connection";
      this.tabData[this.curIndex][1] = "Failure";
      this.tabData[this.curIndex][2] = "";
      this.tabData[this.curIndex][3] = "2";
      this.tabData[this.curIndex][4] = "0";
      this.tabData[this.curIndex][5] = "";
      this.tabData[this.curIndex][6] = this.sqlMessage;
      return;
    }

    int successCount = 0; int failedCount = 0; int discardCount = 0; int existsCount = 0;

    TreePath[] paths = this.tree.getSelectionPaths();
    if (paths == null) return;

    int count2 = 0;
    for (int i = 0; i < paths.length; i++)
    {
      DefaultMutableTreeNode node = (DefaultMutableTreeNode)paths[i].getLastPathComponent();
      if (node == null)
        continue;
      if (!node.isLeaf())
        continue;
      count2++;
    }

    this.tabData = new Object[count2 + 1][7];
    int count = 0;
    for (int i = 0; i < paths.length; i++)
    {
      DefaultMutableTreeNode node = (DefaultMutableTreeNode)paths[i].getLastPathComponent();
      if (node == null) return;

      Object nodeInfo = node.getUserObject();
      if (!node.isLeaf())
        continue;
      PLSQLInfo plsql = (PLSQLInfo)nodeInfo;
      if (plsql.codeStatus.equals("3"))
      {
        discardCount++;
        this.tabData[count][0] = plsql.type;
        this.tabData[count][1] = plsql.schema;
        this.tabData[count][2] = plsql.object;
        this.tabData[count][3] = plsql.codeStatus;
        this.tabData[count][4] = "";
        this.tabData[count][5] = plsql.lineNumber;
        this.tabData[count][6] = "Object was not chosen to be deployed";
      }
      else
      {
        String fileSavedStatus = "";
        if (count2 == 1)
        {
          String md5_a = IBMExtractUtilities.MD5(plsql.plSQLCode);
          String md5_b = IBMExtractUtilities.MD5(this.topArea.getText());
          if (!md5_a.equals(md5_b))
          {
            plsql.plSQLCode = this.topArea.getText();
            fileSavedStatus = IBMExtractUtilities.SaveObject(this.sqlTerminator, this.outputDirectory, plsql.type, plsql.schema, plsql.object, this.topArea.getText());
          }
        }
        IBMExtractUtilities.DeployObject(this.mainConn, this.outputDirectory, plsql.type, plsql.schema, plsql.object, plsql.plSQLCode);
        this.tabData[count][0] = plsql.type;
        this.tabData[count][1] = plsql.schema;
        this.tabData[count][2] = plsql.object;
        if (IBMExtractUtilities.SQLCode.equals(""))
        {
          this.tabData[count][3] = "1";
          plsql.codeStatus = "1";
          this.tabData[count][4] = "Deployed";
          this.tabData[count][5] = "";
          this.tabData[count][6] = fileSavedStatus;
          successCount++;
          IBMExtractUtilities.log(plsql.type + "->" + plsql.schema + "." + plsql.object + " deployed successfully.");
        }
        else if (IBMExtractUtilities.SQLCode.equals("0"))
        {
          this.tabData[count][3] = "2";
          plsql.codeStatus = "2";
          this.tabData[count][4] = IBMExtractUtilities.SQLCode;
          this.tabData[count][5] = "";
          this.tabData[count][6] = "Object already exists in database";
          existsCount++;
          IBMExtractUtilities.log(plsql.type + "->" + plsql.schema + "." + plsql.object + " already exists.");
        }
        else
        {
          this.tabData[count][3] = "0";
          plsql.codeStatus = "0";
          plsql.lineNumber = IBMExtractUtilities.FailedLine;
          this.tabData[count][4] = IBMExtractUtilities.SQLCode;
          this.tabData[count][5] = IBMExtractUtilities.FailedLine;
          this.tabData[count][6] = IBMExtractUtilities.Message;
          failedCount++;
          IBMExtractUtilities.log(plsql.type + "->" + plsql.schema + "." + plsql.object + " deployment failed ");
        }
      }
      node.setUserObject(plsql);
      ((DefaultTreeModel)this.tree.getModel()).nodeStructureChanged(node);
      count++;
    }

    if (count > 0)
    {
      this.tabData[count][0] = ("Deployed=" + successCount);
      this.tabData[count][1] = ("Failed=" + failedCount);
      this.tabData[count][2] = ("Discard=" + discardCount);
      this.tabData[count][3] = "1";
      this.tabData[count][4] = "";
      this.tabData[count][5] = "";
      this.tabData[count][6] = ("Total objects = " + count);
      IBMExtractUtilities.log("Deployed=" + successCount + " Exists already=" + existsCount + " Failed=" + failedCount + " Discard=" + discardCount + " Total objects = " + count);
    }

    IBMExtractUtilities.CloseConnection(this.mainConn);
    IBMExtractUtilities.DeployCompleted = true;
  }

  public void runFull()
  {
    if (this.mainConn == null)
    {
      this.tabData = new Object[1][7];
      this.tabData[this.curIndex][0] = "Connection";
      this.tabData[this.curIndex][1] = "Failure";
      this.tabData[this.curIndex][2] = "";
      this.tabData[this.curIndex][3] = "2";
      this.tabData[this.curIndex][4] = "0";
      this.tabData[this.curIndex][5] = "";
      this.tabData[this.curIndex][6] = this.sqlMessage;
      return;
    }
    int count = getTotalObjects(this.tree);
    this.tabData = new Object[count + 1][7];
    TreeModel model = this.tree.getModel();
    if (model != null)
    {
      this.curIndex = 0;
      Object root = model.getRoot();
      this.tabData = walk(this.tabData, model, root);
      this.tabData[count][0] = ("Deployed=" + this.successCount);
      this.tabData[count][1] = ("Failed=" + this.failedCount);
      this.tabData[count][2] = ("Discard=" + this.discardCount);
      this.tabData[count][3] = "1";
      this.tabData[count][4] = "";
      this.tabData[count][5] = "";
      this.tabData[count][6] = ("Total objects = " + count);
      IBMExtractUtilities.log("Deployed=" + this.successCount + " Exists already=" + this.existsCount + " Failed=" + this.failedCount + " Discard=" + this.discardCount + " Total objects = " + count);
    }
    IBMExtractUtilities.CloseConnection(this.mainConn);
    IBMExtractUtilities.DeployCompleted = true;
    try
    {
      if (!IBMExtractUtilities.FileExists(this.outputDirectory + "/savedobjects"))
      {
        new File(this.outputDirectory + "/savedobjects").mkdir();
      }
      FileOutputStream ostream = new FileOutputStream(this.outputDirectory + "/savedobjects/" + this.DEPLOYED_OBJECT_FILE);
      this.deployedObjectsList.store(ostream, "-- Deployed Objects in DB2");
      ostream.close();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}