package com.jgoodies.forms.factories;

import com.jgoodies.forms.builder.ButtonBarBuilder;
import javax.swing.JButton;
import javax.swing.JPanel;

public final class ButtonBarFactory
{
  public static JPanel buildLeftAlignedBar(JButton button1)
  {
    return buildLeftAlignedBar(new JButton[] { button1 });
  }

  public static JPanel buildLeftAlignedBar(JButton button1, JButton button2)
  {
    return buildLeftAlignedBar(new JButton[] { button1, button2 }, true);
  }

  public static JPanel buildLeftAlignedBar(JButton button1, JButton button2, JButton button3)
  {
    return buildLeftAlignedBar(new JButton[] { button1, button2, button3 }, true);
  }

  public static JPanel buildLeftAlignedBar(JButton button1, JButton button2, JButton button3, JButton button4)
  {
    return buildLeftAlignedBar(new JButton[] { button1, button2, button3, button4 }, true);
  }

  public static JPanel buildLeftAlignedBar(JButton button1, JButton button2, JButton button3, JButton button4, JButton button5)
  {
    return buildLeftAlignedBar(new JButton[] { button1, button2, button3, button4, button5 }, true);
  }

  public static JPanel buildLeftAlignedBar(JButton[] buttons)
  {
    ButtonBarBuilder builder = new ButtonBarBuilder();
    builder.addGriddedButtons(buttons);
    builder.addGlue();
    return builder.getPanel();
  }

  public static JPanel buildLeftAlignedBar(JButton[] buttons, boolean leftToRightButtonOrder)
  {
    ButtonBarBuilder builder = new ButtonBarBuilder();
    builder.setLeftToRightButtonOrder(leftToRightButtonOrder);
    builder.addGriddedButtons(buttons);
    builder.addGlue();
    return builder.getPanel();
  }

  public static JPanel buildCenteredBar(JButton button1)
  {
    return buildCenteredBar(new JButton[] { button1 });
  }

  public static JPanel buildCenteredBar(JButton button1, JButton button2)
  {
    return buildCenteredBar(new JButton[] { button1, button2 });
  }

  public static JPanel buildCenteredBar(JButton button1, JButton button2, JButton button3)
  {
    return buildCenteredBar(new JButton[] { button1, button2, button3 });
  }

  public static JPanel buildCenteredBar(JButton button1, JButton button2, JButton button3, JButton button4)
  {
    return buildCenteredBar(new JButton[] { button1, button2, button3, button4 });
  }

  public static JPanel buildCenteredBar(JButton button1, JButton button2, JButton button3, JButton button4, JButton button5)
  {
    return buildCenteredBar(new JButton[] { button1, button2, button3, button4, button5 });
  }

  public static JPanel buildCenteredBar(JButton[] buttons)
  {
    ButtonBarBuilder builder = new ButtonBarBuilder();
    builder.addGlue();
    builder.addGriddedButtons(buttons);
    builder.addGlue();
    return builder.getPanel();
  }

  public static JPanel buildGrowingBar(JButton button1)
  {
    return buildGrowingBar(new JButton[] { button1 });
  }

  public static JPanel buildGrowingBar(JButton button1, JButton button2)
  {
    return buildGrowingBar(new JButton[] { button1, button2 });
  }

  public static JPanel buildGrowingBar(JButton button1, JButton button2, JButton button3)
  {
    return buildGrowingBar(new JButton[] { button1, button2, button3 });
  }

  public static JPanel buildGrowingBar(JButton button1, JButton button2, JButton button3, JButton button4)
  {
    return buildGrowingBar(new JButton[] { button1, button2, button3, button4 });
  }

  public static JPanel buildGrowingBar(JButton button1, JButton button2, JButton button3, JButton button4, JButton button5)
  {
    return buildGrowingBar(new JButton[] { button1, button2, button3, button4, button5 });
  }

  public static JPanel buildGrowingBar(JButton[] buttons)
  {
    ButtonBarBuilder builder = new ButtonBarBuilder();
    builder.addGriddedGrowingButtons(buttons);
    return builder.getPanel();
  }

  public static JPanel buildRightAlignedBar(JButton button1)
  {
    return buildRightAlignedBar(new JButton[] { button1 });
  }

  public static JPanel buildRightAlignedBar(JButton button1, JButton button2)
  {
    return buildRightAlignedBar(new JButton[] { button1, button2 }, true);
  }

  public static JPanel buildRightAlignedBar(JButton button1, JButton button2, JButton button3)
  {
    return buildRightAlignedBar(new JButton[] { button1, button2, button3 }, true);
  }

  public static JPanel buildRightAlignedBar(JButton button1, JButton button2, JButton button3, JButton button4)
  {
    return buildRightAlignedBar(new JButton[] { button1, button2, button3, button4 }, true);
  }

  public static JPanel buildRightAlignedBar(JButton button1, JButton button2, JButton button3, JButton button4, JButton button5)
  {
    return buildRightAlignedBar(new JButton[] { button1, button2, button3, button4, button5 }, true);
  }

  public static JPanel buildRightAlignedBar(JButton[] buttons)
  {
    ButtonBarBuilder builder = new ButtonBarBuilder();
    builder.addGlue();
    builder.addGriddedButtons(buttons);
    return builder.getPanel();
  }

  public static JPanel buildRightAlignedBar(JButton[] buttons, boolean leftToRightButtonOrder)
  {
    ButtonBarBuilder builder = new ButtonBarBuilder();
    builder.setLeftToRightButtonOrder(leftToRightButtonOrder);
    builder.addGlue();
    builder.addGriddedButtons(buttons);
    return builder.getPanel();
  }

  public static JPanel buildHelpBar(JButton help, JButton button1)
  {
    return buildHelpBar(help, new JButton[] { button1 });
  }

  public static JPanel buildHelpBar(JButton help, JButton button1, JButton button2)
  {
    return buildHelpBar(help, new JButton[] { button1, button2 });
  }

  public static JPanel buildHelpBar(JButton help, JButton button1, JButton button2, JButton button3)
  {
    return buildHelpBar(help, new JButton[] { button1, button2, button3 });
  }

  public static JPanel buildHelpBar(JButton help, JButton button1, JButton button2, JButton button3, JButton button4)
  {
    return buildHelpBar(help, new JButton[] { button1, button2, button3, button4 });
  }

  public static JPanel buildHelpBar(JButton help, JButton[] buttons)
  {
    ButtonBarBuilder builder = new ButtonBarBuilder();
    builder.addGridded(help);
    builder.addRelatedGap();
    builder.addGlue();
    builder.addGriddedButtons(buttons);
    return builder.getPanel();
  }

  public static JPanel buildCloseBar(JButton close)
  {
    return buildRightAlignedBar(close);
  }

  public static JPanel buildOKBar(JButton ok)
  {
    return buildRightAlignedBar(ok);
  }

  public static JPanel buildOKCancelBar(JButton ok, JButton cancel)
  {
    return buildRightAlignedBar(new JButton[] { ok, cancel });
  }

  public static JPanel buildOKCancelApplyBar(JButton ok, JButton cancel, JButton apply)
  {
    return buildRightAlignedBar(new JButton[] { ok, cancel, apply });
  }

  public static JPanel buildHelpCloseBar(JButton help, JButton close)
  {
    return buildHelpBar(help, close);
  }

  public static JPanel buildHelpOKBar(JButton help, JButton ok)
  {
    return buildHelpBar(help, ok);
  }

  public static JPanel buildHelpOKCancelBar(JButton help, JButton ok, JButton cancel)
  {
    return buildHelpBar(help, ok, cancel);
  }

  public static JPanel buildHelpOKCancelApplyBar(JButton help, JButton ok, JButton cancel, JButton apply)
  {
    return buildHelpBar(help, ok, cancel, apply);
  }

  public static JPanel buildCloseHelpBar(JButton close, JButton help)
  {
    return buildRightAlignedBar(new JButton[] { close, help });
  }

  public static JPanel buildOKHelpBar(JButton ok, JButton help)
  {
    return buildRightAlignedBar(new JButton[] { ok, help });
  }

  public static JPanel buildOKCancelHelpBar(JButton ok, JButton cancel, JButton help)
  {
    return buildRightAlignedBar(new JButton[] { ok, cancel, help });
  }

  public static JPanel buildOKCancelApplyHelpBar(JButton ok, JButton cancel, JButton apply, JButton help)
  {
    return buildRightAlignedBar(new JButton[] { ok, cancel, apply, help });
  }

  public static JPanel buildAddRemoveLeftBar(JButton add, JButton remove)
  {
    return buildLeftAlignedBar(add, remove);
  }

  public static JPanel buildAddRemoveBar(JButton add, JButton remove)
  {
    return buildGrowingBar(add, remove);
  }

  public static JPanel buildAddRemoveRightBar(JButton add, JButton remove)
  {
    return buildRightAlignedBar(add, remove);
  }

  public static JPanel buildAddRemovePropertiesLeftBar(JButton add, JButton remove, JButton properties)
  {
    return buildLeftAlignedBar(add, remove, properties);
  }

  public static JPanel buildAddRemovePropertiesBar(JButton add, JButton remove, JButton properties)
  {
    ButtonBarBuilder builder = new ButtonBarBuilder();
    builder.addGriddedGrowing(add);
    builder.addRelatedGap();
    builder.addGriddedGrowing(remove);
    builder.addRelatedGap();
    builder.addGriddedGrowing(properties);
    return builder.getPanel();
  }

  public static JPanel buildAddRemovePropertiesRightBar(JButton add, JButton remove, JButton properties)
  {
    return buildRightAlignedBar(add, remove, properties);
  }

  public static JPanel buildWizardBar(JButton back, JButton next, JButton finish, JButton cancel)
  {
    return buildWizardBar(back, next, new JButton[] { finish, cancel });
  }

  public static JPanel buildWizardBar(JButton help, JButton back, JButton next, JButton finish, JButton cancel)
  {
    return buildWizardBar(new JButton[] { help }, back, next, new JButton[] { finish, cancel });
  }

  public static JPanel buildWizardBar(JButton back, JButton next, JButton[] rightAlignedButtons)
  {
    return buildWizardBar(null, back, next, rightAlignedButtons);
  }

  public static JPanel buildWizardBar(JButton[] leftAlignedButtons, JButton back, JButton next, JButton[] rightAlignedButtons)
  {
    return buildWizardBar(leftAlignedButtons, back, next, null, rightAlignedButtons);
  }

  public static JPanel buildWizardBar(JButton[] leftAlignedButtons, JButton back, JButton next, JButton overlaidFinish, JButton[] rightAlignedButtons)
  {
    ButtonBarBuilder builder = new ButtonBarBuilder();
    if (leftAlignedButtons != null) {
      builder.addGriddedButtons(leftAlignedButtons);
      builder.addRelatedGap();
    }
    builder.addGlue();
    builder.addGridded(back);
    builder.addGridded(next);

    if (overlaidFinish != null) {
      builder.nextColumn(-1);
      builder.add(overlaidFinish);
      builder.nextColumn();
    }

    if (rightAlignedButtons != null) {
      builder.addRelatedGap();
      builder.addGriddedButtons(rightAlignedButtons);
    }
    return builder.getPanel();
  }
}