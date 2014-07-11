package com.jgoodies.uif_lite.panel;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.GradientPaint;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Insets;
import java.awt.LayoutManager;
import java.awt.Paint;
import javax.swing.BorderFactory;
import javax.swing.Icon;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JToolBar;
import javax.swing.UIManager;
import javax.swing.border.AbstractBorder;

public class SimpleInternalFrame extends JPanel {
	private final JLabel titleLabel;
	private GradientPanel gradientPanel;
	private JPanel headerPanel;
	private boolean selected;

	public SimpleInternalFrame() {
		this("Title");
	}

	public SimpleInternalFrame(String title) {
		this(null, title, null, null);
	}

	public SimpleInternalFrame(Icon icon, String title) {
		this(icon, title, null, null);
	}

	public SimpleInternalFrame(String title, JToolBar bar, JComponent content) {
		this(null, title, bar, content);
	}

	public SimpleInternalFrame(Icon icon, String title, JToolBar bar,
			JComponent content) {
		super(new BorderLayout());
		this.selected = false;
		this.titleLabel = new JLabel(title, icon, 10);
		JPanel top = buildHeader(this.titleLabel, bar);

		add(top, "North");
		if (content != null) {
			setContent(content);
		}
		setBorder(new ShadowBorder());
		setSelected(true);
		updateHeader();
	}

	public Icon getFrameIcon() {
		return this.titleLabel.getIcon();
	}

	public void setFrameIcon(Icon newIcon) {
		Icon oldIcon = getFrameIcon();
		this.titleLabel.setIcon(newIcon);
		firePropertyChange("frameIcon", oldIcon, newIcon);
	}

	public String getTitle() {
		return this.titleLabel.getText();
	}

	public void setTitle(String newText) {
		String oldText = getTitle();
		this.titleLabel.setText(newText);
		firePropertyChange("title", oldText, newText);
	}

	public JToolBar getToolBar() {
		return this.headerPanel.getComponentCount() > 1 ? (JToolBar) this.headerPanel
				.getComponent(1)
				: null;
	}

	public void setToolBar(JToolBar newToolBar) {
		JToolBar oldToolBar = getToolBar();
		if (oldToolBar == newToolBar) {
			return;
		}
		if (oldToolBar != null) {
			this.headerPanel.remove(oldToolBar);
		}
		if (newToolBar != null) {
			newToolBar.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 0));
			this.headerPanel.add(newToolBar, "East");
		}
		updateHeader();
		firePropertyChange("toolBar", oldToolBar, newToolBar);
	}

	public Component getContent() {
		return hasContent() ? getComponent(1) : null;
	}

	public void setContent(Component newContent) {
		Component oldContent = getContent();
		if (hasContent()) {
			remove(oldContent);
		}
		add(newContent, "Center");
		firePropertyChange("content", oldContent, newContent);
	}

	public boolean isSelected() {
		return this.selected;
	}

	public void setSelected(boolean newValue) {
		boolean oldValue = isSelected();
		this.selected = newValue;
		updateHeader();
		firePropertyChange("selected", oldValue, newValue);
	}

	private JPanel buildHeader(JLabel label, JToolBar bar) {
		this.gradientPanel = new GradientPanel(new BorderLayout(),
				getHeaderBackground());

		label.setOpaque(false);

		this.gradientPanel.add(label, "West");
		this.gradientPanel.setBorder(BorderFactory
				.createEmptyBorder(3, 4, 3, 1));

		this.headerPanel = new JPanel(new BorderLayout());
		this.headerPanel.add(this.gradientPanel, "Center");
		setToolBar(bar);
		this.headerPanel.setBorder(new RaisedHeaderBorder());
		this.headerPanel.setOpaque(false);
		return this.headerPanel;
	}

	private void updateHeader() {
		this.gradientPanel.setBackground(getHeaderBackground());
		this.gradientPanel.setOpaque(isSelected());
		this.titleLabel.setForeground(getTextForeground(isSelected()));
		this.headerPanel.repaint();
	}

	public void updateUI() {
		super.updateUI();
		if (this.titleLabel != null)
			updateHeader();
	}

	private boolean hasContent() {
		return getComponentCount() > 1;
	}

	protected Color getTextForeground(boolean isSelected) {
		Color c = UIManager
				.getColor(isSelected ? "SimpleInternalFrame.activeTitleForeground"
						: "SimpleInternalFrame.inactiveTitleForeground");

		if (c != null) {
			return c;
		}
		return UIManager
				.getColor(isSelected ? "InternalFrame.activeTitleForeground"
						: "Label.foreground");
	}

	protected Color getHeaderBackground() {
		Color c = UIManager
				.getColor("SimpleInternalFrame.activeTitleBackground");

		return c != null ? c : UIManager
				.getColor("InternalFrame.activeTitleBackground");
	}

	private static final class GradientPanel extends JPanel {
		private GradientPanel(LayoutManager lm, Color background) {
			super();
			setBackground(background);
		}

		public void paintComponent(Graphics g) {
			super.paintComponent(g);
			if (!isOpaque()) {
				return;
			}
			Color control = UIManager.getColor("control");
			int width = getWidth();
			int height = getHeight();

			Graphics2D g2 = (Graphics2D) g;
			Paint storedPaint = g2.getPaint();
			g2.setPaint(new GradientPaint(0.0F, 0.0F, getBackground(), width,
					0.0F, control));

			g2.fillRect(0, 0, width, height);
			g2.setPaint(storedPaint);
		}
	}

	private static class ShadowBorder extends AbstractBorder {
		private static final Insets INSETS = new Insets(1, 1, 3, 3);

		public Insets getBorderInsets(Component c) {
			return INSETS;
		}

		public void paintBorder(Component c, Graphics g, int x, int y, int w,
				int h) {
			Color shadow = UIManager.getColor("controlShadow");
			if (shadow == null) {
				shadow = Color.GRAY;
			}
			Color lightShadow = new Color(shadow.getRed(), shadow.getGreen(),
					shadow.getBlue(), 170);

			Color lighterShadow = new Color(shadow.getRed(), shadow.getGreen(),
					shadow.getBlue(), 70);

			g.translate(x, y);

			g.setColor(shadow);
			g.fillRect(0, 0, w - 3, 1);
			g.fillRect(0, 0, 1, h - 3);
			g.fillRect(w - 3, 1, 1, h - 3);
			g.fillRect(1, h - 3, w - 3, 1);

			g.setColor(lightShadow);
			g.fillRect(w - 3, 0, 1, 1);
			g.fillRect(0, h - 3, 1, 1);
			g.fillRect(w - 2, 1, 1, h - 3);
			g.fillRect(1, h - 2, w - 3, 1);

			g.setColor(lighterShadow);
			g.fillRect(w - 2, 0, 1, 1);
			g.fillRect(0, h - 2, 1, 1);
			g.fillRect(w - 2, h - 2, 1, 1);
			g.fillRect(w - 1, 1, 1, h - 2);
			g.fillRect(1, h - 1, w - 2, 1);
			g.translate(-x, -y);
		}
	}

	private static class RaisedHeaderBorder extends AbstractBorder {
		private static final Insets INSETS = new Insets(1, 1, 1, 0);

		public Insets getBorderInsets(Component c) {
			return INSETS;
		}

		public void paintBorder(Component c, Graphics g, int x, int y, int w,
				int h) {
			g.translate(x, y);
			g.setColor(UIManager.getColor("controlLtHighlight"));
			g.fillRect(0, 0, w, 1);
			g.fillRect(0, 1, 1, h - 1);
			g.setColor(UIManager.getColor("controlShadow"));
			g.fillRect(0, h - 1, w, 1);
			g.translate(-x, -y);
		}
	}
}