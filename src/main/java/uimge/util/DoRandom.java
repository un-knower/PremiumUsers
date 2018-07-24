package uimge.util;

public class DoRandom {

    public int[] dice(String[] keys, double[] values) {

        int[] res = new int[keys.length];
        int temp;
        do {
            temp = 0;
            for (int i = 0; i < keys.length; i++) {
                res[i] = (((Math.random() / (1 - values[i])) >= 1) ? 1 : 0);
                temp += res[i];
            }
        } while (temp != 1);
        return res;
    }

    public int[] diceOnce(String[] keys, double[] values) {

        int[] res = new int[keys.length];
        int temp = 0;
        for (int i = 0; i < keys.length; i++) {
            res[i] = (((Math.random() / (1 - values[i])) >= 1) ? 1 : 0);
            temp += res[i];
        }
        return res;
    }
}