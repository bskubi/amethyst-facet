from hypothesis import strategies as st
import polars as pl

@st.composite
def chromnames(draw):
    integers = draw(st.booleans())
    format = draw(st.sampled_from(["{0}", "chr{0}"]))
    base = st.integers() if integers else st.text()
    formatted = base.map(lambda chrom: format.format(chrom))
    chromnames = st.lists(formatted, unique=True, min_size=1)
    return draw(chromnames)

@st.composite
def dense_uniform_observations(draw):
    state = dict()
    try:
        size = draw(st.integers(min_value=1, max_value=100))
        state["size"] = size
        step = size
        state["step"] = step
        offset = draw(st.integers(min_value=-1000, max_value=1000))
        state["offset"] = offset
        observations_count = draw(st.integers(min_value=0, max_value=1000))*size
        state["observations_count"] = observations_count
        
        c_val = draw(st.integers(min_value=0, max_value=1000))
        t_val = draw(st.integers(min_value=0, max_value=1000))
        chr = ["chr1"]*observations_count
        pos = range(offset, offset + observations_count, 1)
        c = [c_val]*observations_count
        t = [t_val]*observations_count
        
        observations = pl.DataFrame({
            "chr": chr,
            "pos": pos,
            "c": c,
            "t": t
        })
        state["observations"] = observations
        windows_count = observations_count // size
        state["windows_count"] = windows_count

        chr = ["chr1"]*windows_count
        start = list(range(offset, max(pos) + 1, step)) if pos else []
        state["len(start)"] = len(start)
        end = list(range(offset + size, max(pos) + 1 + size, step)) if pos else []
        state["len(end)"] = len(start)
        c = [c_val*size]*windows_count
        t = [t_val*size]*windows_count
        c_nz = [(c_val > 0)*size]*windows_count
        t_nz = [(t_val > 0)*size]*windows_count

        expected = pl.DataFrame({
            "chr": chr,
            "start": start,
            "end": end,
            "c": c,
            "t": t,
            "c_nz": c_nz,
            "t_nz": t_nz
        })
        state["expected"] = expected
    except:
        print("--------------------")
        for k, v in state.items():
            print(k)
            print(v)
        print("--------------------")
        raise

    return state