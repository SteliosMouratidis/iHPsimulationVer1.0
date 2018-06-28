package cassandra;

public class Tuple<A, B> {
	public final A a;
	public final B b;


	public Tuple(A a, B b) {
		this.a = a;
		this.b = b;

	}
/*
	@Override
	public String toString() {
		return "(" + x + "," + y + ")";
	}

	@Override
	public boolean equals(Object other) {
		if (other == this) {
			return true;
		}

		if (!(other instanceof Tuple)) {
			return false;
		}

		@SuppressWarnings("unchecked")
		Tuple<X, Y> other_ = (Tuple<X, Y>) other;
		return other_.x.equals(this.x) && other_.y.equals(this.y);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((x == null) ? 0 : x.hashCode());
		result = prime * result + ((y == null) ? 0 : y.hashCode());
		return result;
	}
*/
}
