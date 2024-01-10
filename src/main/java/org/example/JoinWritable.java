/**
 * projectName: MapReduce-Join
 * fileName: JoinWritable.java
 * packageName: org.example
 * date: 2024-01-10 16:06
 */
package org.example;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: QSky
 * @data: 2024-01-10 16:06
 **/
public class JoinWritable implements Writable {
    private Long OrderId = 0L;
    private String Account = "";
    private String Date = "";
    private Long ItemId = 0L;
    private Long Num = 0L;

    public void setOrderId(Long orderId) {
        OrderId = orderId;
    }

    public void setAccount(String account) {
        Account = account;
    }

    public void setDate(String date) {
        Date = date;
    }

    public void setItemId(Long itemId) {
        ItemId = itemId;
    }

    public void setNum(Long num) {
        Num = num;
    }

    @Override
    public String toString() {
        return "OrderId= " + OrderId +
                " Account= '" + Account + '\'' +
                " Date= '" + Date + '\'' +
                " ItemId= " + ItemId +
                " Num= " + Num;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.OrderId);
        dataOutput.writeUTF(this.Account);
        dataOutput.writeUTF(this.Date);
        dataOutput.writeLong(this.ItemId);
        dataOutput.writeLong(this.Num);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.OrderId = dataInput.readLong();
        this.Account = dataInput.readUTF();
        this.Date = dataInput.readUTF();
        this.ItemId = dataInput.readLong();
        this.Num = dataInput.readLong();
    }
}