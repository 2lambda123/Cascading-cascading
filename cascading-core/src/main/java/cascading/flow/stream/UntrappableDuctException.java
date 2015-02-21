package cascading.flow.stream;

public class UntrappableDuctException extends DuctException {

  public UntrappableDuctException( String message, Throwable throwable )
    {
    super( message, throwable );
    }

}
