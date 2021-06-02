public class MyThread {
    public static void main(String[] args) {
        Thread t = new Thread(){
            public void run (){
                System.out.println(currentThread().getName());
            }
        };
        t.start();//在main主线程中启动了一个线程，
        //t.run();//相当于在main的线程中运行一个方法，
        System.out.println("hello");
    }
}
