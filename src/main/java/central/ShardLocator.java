/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/** @author alulo */
package central;

public final class ShardLocator {
    private final int numParts;
    public ShardLocator(int n) { this.numParts = n; }

    public int shardFor(long idCuenta) {
        long h = (idCuenta * 2654435761L) ^ (idCuenta >>> 16); // hash simple determinista
        if (h < 0) h = -h;
        return (int)(h % numParts); // 0..numParts-1
    }
}