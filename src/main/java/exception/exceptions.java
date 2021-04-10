package exception;

public class exceptions {

    public static class NotQueryException extends RuntimeException {
        private static final String message = "只允许输入SELECT语句，请填写正确的SQL";

        public NotQueryException() {
            super(message);
        }
    }

}



