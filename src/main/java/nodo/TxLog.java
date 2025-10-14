/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/** @author alulo */
package nodo;

import java.util.concurrent.ConcurrentHashMap;

/** Idempotencia por tx_id (en memoria). Se puede extender a archivo CSV. */
public class TxLog {
    private final ConcurrentHashMap<String, String> log = new ConcurrentHashMap<>();
    public boolean seen(String txId) { return log.containsKey(txId); }
    public void put(String txId, String state) { log.put(txId, state); }
}
