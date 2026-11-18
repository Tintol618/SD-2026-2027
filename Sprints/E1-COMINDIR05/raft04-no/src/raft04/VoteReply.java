package raft04;

// VOTO;<termo>;<eleitor>;<candidato>;<S|N>
public final class VoteReply {
    public final long term;
    public final int voter;
    public final int candidate;
    public final boolean granted;

    public VoteReply(long term, int voter, int candidate, boolean granted) {
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    public String encode() {
        return "VOTO;" + term + ";" + voter + ";" + candidate + ";" + (granted ? "S" : "N");
    }

    public static VoteReply decode(String msg) {
        String[] f = msg.trim().split(";");
        if (f.length != 5 || !f[0].equals("VOTO") || !(f[4].equals("S") || f[4].equals("N"))) {
            throw new IllegalArgumentException("VOTO mal formado: " + msg);
        }
        VoteReply v = new VoteReply(Long.parseLong(f[1]), Integer.parseInt(f[2]), Integer.parseInt(f[3]), f[4].equals("S"));
        if (v.term < 0 || v.voter < 1 || v.candidate < 1) {
            throw new IllegalArgumentException("VOTO inválido: " + msg);
        }
        return v;
    }
}
