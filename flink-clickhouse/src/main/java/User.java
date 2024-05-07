import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zongkxc
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {
    public String id;
    public String name;
    public String age;
    public static User of(String id, String name, String age) {
        return new User(id, name, age);
    }
}
