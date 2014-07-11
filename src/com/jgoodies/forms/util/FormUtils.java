package com.jgoodies.forms.util;

import javax.swing.LookAndFeel;
import javax.swing.UIManager;

public final class FormUtils {

	private FormUtils() {
	}

	public static void assertNotBlank(String text, String description) {
		if (text == null)
			throw new NullPointerException((new StringBuilder()).append("The ")
					.append(description).append(" must not be null.")
					.toString());
		if (isBlank(text))
			throw new IllegalArgumentException((new StringBuilder()).append(
					"The ").append(description).append(
					" must not be empty, or whitespace. ").append(
					"See FormUtils.isBlank(String)").toString());
		else
			return;
	}

	public static void assertNotNull(Object object, String description) {
		if (object == null)
			throw new NullPointerException((new StringBuilder()).append("The ")
					.append(description).append(" must not be null.")
					.toString());
		else
			return;
	}

	public static boolean equals(Object o1, Object o2) {
		return o1 != null && o2 != null && o1.equals(o2) || o1 == null
				&& o2 == null;
	}

	public static boolean isBlank(String str) {
		int length;
		if (str == null || (length = str.length()) == 0)
			return true;
		for (int i = length - 1; i >= 0; i--)
			if (!Character.isWhitespace(str.charAt(i)))
				return false;

		return true;
	}

	public static boolean isNotBlank(String str) {
		int length;
		if (str == null || (length = str.length()) == 0)
			return false;
		for (int i = length - 1; i >= 0; i--)
			if (!Character.isWhitespace(str.charAt(i)))
				return true;

		return false;
	}

	public static boolean isLafAqua() {
		ensureValidCache();
		if (cachedIsLafAqua == null)
			cachedIsLafAqua = Boolean.valueOf(computeIsLafAqua());
		return cachedIsLafAqua.booleanValue();
	}

	public static void clearLookAndFeelBasedCaches() {
		cachedIsLafAqua = null;
		DefaultUnitConverter.getInstance().clearCache();
	}

	private static boolean computeIsLafAqua() {
		return UIManager.getLookAndFeel().getID().equals("Aqua");
	}

	static void ensureValidCache() {
		LookAndFeel currentLookAndFeel = UIManager.getLookAndFeel();
		if (currentLookAndFeel != cachedLookAndFeel) {
			clearLookAndFeelBasedCaches();
			cachedLookAndFeel = currentLookAndFeel;
		}
	}

	private static LookAndFeel cachedLookAndFeel;
	private static Boolean cachedIsLafAqua;
}

/*
 * DECOMPILATION REPORT
 * 
 * Decompiled from: D:\workspaces\MyEclipse
 * 8.6\IBMDataMovementTool\lib\IBMDataMovementTool.jar Total time: 16 ms Jad
 * reported messages/errors: Exit status: 0 Caught exceptions:
 */