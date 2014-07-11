package ibm;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import javax.swing.JComponent;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

class CustomJTree extends JTree
  implements ActionListener
{
  private static final long serialVersionUID = 1273279053102663185L;
  JPopupMenu popup;
  JPopupMenu popup2;
  JMenuItem mi;

  public CustomJTree(DefaultMutableTreeNode node, ActionListener executeAllActionListener, ActionListener revalidateAllActionListener, ActionListener executeActionListener, ActionListener revalidateActionListener, ActionListener discardActionListener)
  {
    super(node);
    this.popup = new JPopupMenu();

    this.mi = new JMenuItem("Deploy selected objects in DB2");
    this.mi.addActionListener(executeActionListener);
    this.mi.setActionCommand("deploy");
    this.popup.add(this.mi);
    this.mi = new JMenuItem("Revalidate selected objects in DB2");
    this.mi.addActionListener(revalidateActionListener);
    this.mi.setActionCommand("revalidate");
    this.popup.add(this.mi);
    this.mi = new JMenuItem("Do not deploy selected objects in DB2");
    this.mi.addActionListener(discardActionListener);
    this.mi.setActionCommand("discard");
    this.popup.add(this.mi);
    this.popup.setOpaque(true);
    this.popup.setLightWeightPopupEnabled(true);

    this.popup2 = new JPopupMenu();
    this.mi = new JMenuItem("Expand All");
    this.mi.addActionListener(this);
    this.mi.setActionCommand("expand");
    this.popup2.add(this.mi);

    this.mi = new JMenuItem("Collapse All");
    this.mi.addActionListener(this);
    this.mi.setActionCommand("collapse");
    this.popup2.add(this.mi);

    this.popup2.addSeparator();

    this.mi = new JMenuItem("Deploy All objects in DB2");
    this.mi.addActionListener(executeAllActionListener);
    this.mi.setActionCommand("deployAll");
    this.popup2.add(this.mi);
    this.mi = new JMenuItem("Revalidate All objects in DB2");
    this.mi.addActionListener(revalidateAllActionListener);
    this.mi.setActionCommand("revalidateAll");
    this.popup2.add(this.mi);
    this.popup2.setOpaque(true);
    this.popup2.setLightWeightPopupEnabled(true);

    addMouseListener(new MouseAdapter()
    {
      public void mousePressed(MouseEvent e) {
        showPopup(e);
      }

      public void mouseReleased(MouseEvent e) {
        showPopup(e);
      }

      private void showPopup(MouseEvent e) {
        if (e.isPopupTrigger())
        {
          TreePath path = CustomJTree.this.getSelectionPath();
          if (path != null)
          {
            DefaultMutableTreeNode node = (DefaultMutableTreeNode)path.getLastPathComponent();
            if (node.isLeaf())
            {
              CustomJTree.this.popup.show((JComponent)e.getSource(), e.getX(), e.getY());
            }
            else
              CustomJTree.this.popup2.show((JComponent)e.getSource(), e.getX(), e.getY());
          }
        }
      }
    });
  }

  public void actionPerformed(ActionEvent e)
  {
    TreeModel data = getModel();
    Object node = data.getRoot();
    if (node == null) return;

    if (e.getActionCommand().equals("expand"))
    {
      int row = 0;
      while (row < getRowCount())
      {
        expandRow(row);
        row++;
      }
    }
    if (e.getActionCommand().equals("collapse"))
    {
      int row = getRowCount() - 1;
      while (row >= 0)
      {
        collapseRow(row);
        row--;
      }
    }
  }
}