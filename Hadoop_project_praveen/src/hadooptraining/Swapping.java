package hadooptraining;

import java.util.Scanner;

public class Swapping {

	public static void main(String[] args) {

		Scanner s = new Scanner(System.in);
		System.out.println("enter a value=");
		int a = s.nextInt();

		System.out.println("enter b value=");
		int b = s.nextInt();
		System.out.println("a====" + a);
		System.out.println("b======" + b);
		int c = a;
		a = b;
		b = c;
		System.out.println("a====" + a);
		System.out.println("b======" + b);

	}

}
