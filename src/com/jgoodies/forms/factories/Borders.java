package com.jgoodies.forms.factories;

import java.awt.Component;
import java.awt.Insets;

import javax.swing.border.AbstractBorder;
import javax.swing.border.Border;

import com.jgoodies.forms.layout.ConstantSize;
import com.jgoodies.forms.layout.Sizes;
import com.jgoodies.forms.util.LayoutStyle;

public final class Borders {
	public static final class EmptyBorder extends AbstractBorder {

		public Insets getBorderInsets(Component c, Insets insets) {
			insets.top = top.getPixelSize(c);
			insets.left = left.getPixelSize(c);
			insets.bottom = bottom.getPixelSize(c);
			insets.right = right.getPixelSize(c);
			return insets;
		}

		public Insets getBorderInsets(Component c) {
			return getBorderInsets(c, new Insets(0, 0, 0, 0));
		}

		public ConstantSize top() {
			return top;
		}

		public ConstantSize left() {
			return left;
		}

		public ConstantSize bottom() {
			return bottom;
		}

		public ConstantSize right() {
			return right;
		}

		private final ConstantSize top;
		private final ConstantSize left;
		private final ConstantSize bottom;
		private final ConstantSize right;

		private EmptyBorder(ConstantSize top, ConstantSize left,
				ConstantSize bottom, ConstantSize right) {
			if (top == null || left == null || bottom == null || right == null) {
				throw new NullPointerException(
						"The top, left, bottom, and right must not be null.");
			} else {
				this.top = top;
				this.left = left;
				this.bottom = bottom;
				this.right = right;
				return;
			}
		}

	}

	private Borders() {
	}

	public static Border createEmptyBorder(ConstantSize top, ConstantSize left,
			ConstantSize bottom, ConstantSize right) {
		return new EmptyBorder(top, left, bottom, right);
	}

	public static Border createEmptyBorder(String encodedSizes) {
		String token[] = encodedSizes.split("\\s*,\\s*");
		int tokenCount = token.length;
		if (token.length != 4) {
			throw new IllegalArgumentException((new StringBuilder()).append(
					"The border requires 4 sizes, but \"").append(encodedSizes)
					.append("\" has ").append(tokenCount).append(".")
					.toString());
		} else {
			ConstantSize top = Sizes.constant(token[0], false);
			ConstantSize left = Sizes.constant(token[1], true);
			ConstantSize bottom = Sizes.constant(token[2], false);
			ConstantSize right = Sizes.constant(token[3], true);
			return createEmptyBorder(top, left, bottom, right);
		}
	}

	public static final Border EMPTY_BORDER = new javax.swing.border.EmptyBorder(
			0, 0, 0, 0);
	public static final Border DLU2_BORDER;
	public static final Border DLU4_BORDER;
	public static final Border DLU7_BORDER;
	public static final Border DLU14_BORDER;
	public static final Border DLU21_BORDER;
	public static final Border BUTTON_BAR_GAP_BORDER = createEmptyBorder(
			LayoutStyle.getCurrent().getButtonBarPad(), Sizes.dluX(0), Sizes
					.dluY(0), Sizes.dluX(0));
	public static final Border DIALOG_BORDER = createEmptyBorder(LayoutStyle
			.getCurrent().getDialogMarginY(), LayoutStyle.getCurrent()
			.getDialogMarginX(), LayoutStyle.getCurrent().getDialogMarginY(),
			LayoutStyle.getCurrent().getDialogMarginX());
	public static final Border TABBED_DIALOG_BORDER = createEmptyBorder(
			LayoutStyle.getCurrent().getTabbedDialogMarginY(), LayoutStyle
					.getCurrent().getTabbedDialogMarginX(), LayoutStyle
					.getCurrent().getTabbedDialogMarginY(), LayoutStyle
					.getCurrent().getTabbedDialogMarginX());

	static {
		DLU2_BORDER = createEmptyBorder(Sizes.DLUY2, Sizes.DLUX2, Sizes.DLUY2,
				Sizes.DLUX2);
		DLU4_BORDER = createEmptyBorder(Sizes.DLUY4, Sizes.DLUX4, Sizes.DLUY4,
				Sizes.DLUX4);
		DLU7_BORDER = createEmptyBorder(Sizes.DLUY7, Sizes.DLUX7, Sizes.DLUY7,
				Sizes.DLUX7);
		DLU14_BORDER = createEmptyBorder(Sizes.DLUY14, Sizes.DLUX14,
				Sizes.DLUY14, Sizes.DLUX14);
		DLU21_BORDER = createEmptyBorder(Sizes.DLUY21, Sizes.DLUX21,
				Sizes.DLUY21, Sizes.DLUX21);
	}
}