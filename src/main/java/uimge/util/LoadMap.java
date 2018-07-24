package uimge.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public final class LoadMap {
    public Map<String, String> load(String filePath) {
        Map<String, String> alline = new HashMap<>();
        try {
            InputStream is = this.getClass().getResourceAsStream(filePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String tempString;
            while ((tempString = br.readLine()) != null) {
                alline.put(tempString.split("=")[0], tempString.split("=")[1]);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return alline;
    }
}
