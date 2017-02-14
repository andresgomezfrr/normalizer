package rb.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class MapperModel {

    public List<String> dimPath;
    public String as;

    @JsonCreator
    public MapperModel(@JsonProperty("dimPath") List<String> dimPath,
                       @JsonProperty("as") String as) {
        this.dimPath = dimPath;

        if (as != null ) {
            this.as = as;
        } else if(dimPath != null) {
            this.as = dimPath.get(dimPath.size() - 1);
        }
    }

    @JsonProperty
    public List<String> getDimPath() {
        return dimPath;
    }

    @JsonProperty
    public String getAs() {
        return as;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("dimPath: ").append(dimPath).append(", ")
                .append("as: ").append(as)
                .append("}");

        return builder.toString();
    }
}
