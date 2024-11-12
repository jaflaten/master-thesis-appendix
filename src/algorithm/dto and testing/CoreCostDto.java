package alien.priority;

public class CoreCostDto {
    private int cores;
    private float cost;

    public int getCores() {
        return cores;
    }

    public void setCores(int cores) {
        this.cores = cores;
    }

    public float getCost() {
        return cost;
    }

    public void setCost(float cost) {
        this.cost = cost;
    }

    public CoreCostDto(int cores, float cost) {
        this.cores = cores;
        this.cost = cost;
    }

    public CoreCostDto() {
    }

    @Override
    public String toString() {
        return "CoreCostDto{" +
                "cores=" + cores +
                ", cost=" + cost +
                '}';
    }
}
