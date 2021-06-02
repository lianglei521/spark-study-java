/**
 * author:liangsir
 * qq:714628767
 * created 2019/03/06.
 */
public class Prog {
    public static void main(String[] args) {
        int i = 1;
        int sum = 0,fact = 1;
        while(i <= 10){
            for(int j = 1;j<=i;j++){
            fact = fact * j;
            }
            sum = sum + fact;
            i++;
            fact = 1;
        }
        System.out.println(sum);
    }
}
