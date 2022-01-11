package com.yhh.dao;

import com.yhh.constant.DelayConst;
import com.yhh.dto.DelayDto;
import com.yhh.utils.DateUtils;
import com.yhh.utils.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.JDBC;
import org.sqlite.SQLiteDataSource;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * @author yhh 2021-12-22 22:02:18
 **/
public class DelayDao {
    private static final Logger log = LoggerFactory.getLogger(DelayDao.class);
    /**
     * sqlite 禁止并发写
     */
    private final Object writeLock = new Object();
    /**
     * 建表 sql 语句
     */
    private static final String SQL_CREATE_TABLE = "CREATE TABLE if NOT EXISTS common_delay_msg(id INTEGER PRIMARY KEY NOT NULL ,topic TEXT NOT NULL,message_key TEXT,message TEXT NOT NULL,delay_epoch INTEGER NOT NULL)";
    /**
     * 建立索引 sql 语句
     */
    private static final String SQL_INDEX = "CREATE INDEX idx_delay_epoch ON common_delay_msg(delay_epoch);";
    /**
     * 保存本地延时消息 sql 语句
     */
    private static final String SQL_ADD = "INSERT INTO common_delay_msg (id,topic,message_key,message,delay_epoch) VALUES (?,?,?,?,?);";
    /**
     * 删除本地延时消息 sql 语句
     */
    private static final String SQL_DELETE = "DELETE FROM common_delay_msg WHERE id = ? ;";
    /**
     * 扫描本地延时消息sql
     */
    private static final String SQL_SCAN = "SELECT id,topic,message_key,message FROM common_delay_msg WHERE delay_epoch <= ? limit 10000";
    /**
     * 数据源
     */
    private final SQLiteDataSource dataSource = new SQLiteDataSource();

    private void initSQLiteDataSource(int dbId) {
        String path = DelayConst.STORE_PATH + "/" + dbId;
        File file = new File(path);
        file.mkdirs();
        dataSource.setReadUncommited(true);
        dataSource.setJournalMode("WAL");
        dataSource.setSynchronous("NORMAL");
        // 追求极限性能，可以切换同步方式为 OFF ，但这个模式在操作系统奔溃或电源异常时，有概率造成sqlite数据库损坏
        // dataSource.setSynchronous("OFF");
        dataSource.setTempStore("MEMORY");
        dataSource.setUrl(JDBC.PREFIX + path + "/delayMsg.db");
        try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()
        ) {
            int i = statement.executeUpdate(SQL_CREATE_TABLE);
            if (i == 1) {
                statement.executeUpdate(SQL_INDEX);
                log.info("成功初始化 sqlite 表 common_delay_msg");
            } else {
                log.debug("sqlite 表 common_delay_msg 已存在");
            }
        } catch (Exception e) {
            log.error("initSQLiteDataSource[初始化sqlite失败] || path : {} ", path, e);
            ExitUtils.exit();
        }
    }

    public DelayDao(int dbId) {
        initSQLiteDataSource(dbId);
    }

    /**
     * 扫描到达延期时限的消息
     *
     * @param records 延时记录
     */
    public void scanTodoMsg(List<DelayDto> records) {
        // long start = System.currentTimeMillis();
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(SQL_SCAN)
        ) {
            statement.setLong(1, DateUtils.epochSecond());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    DelayDto delayDto = new DelayDto();
                    delayDto.setId(resultSet.getLong(1));
                    delayDto.setTopic(resultSet.getString(2));
                    delayDto.setMessageKey(resultSet.getString(3));
                    delayDto.setMessage(resultSet.getString(4));
                    records.add(delayDto);
                }
            }
        } catch (Exception e) {
            log.error("scanTodoMsg[本地数据库异常]", e);
            ExitUtils.exit();
        }
        // log.info("scanTodoMsg || 耗时: {} ms ", System.currentTimeMillis() - start);
    }

    /**
     * 批量存储延时消息到本地数据库
     *
     * @param records
     */
    public void batchStore(List<DelayDto> records) {
        synchronized (writeLock) {
            try (
                    Connection connection = dataSource.getConnection();
                    PreparedStatement preparedStatement = connection.prepareStatement(SQL_ADD)
            ) {
                connection.setAutoCommit(false);
                for (DelayDto record : records) {
                    preparedStatement.setLong(1, record.getId());
                    preparedStatement.setString(2, record.getTopic());
                    preparedStatement.setString(3, record.getMessageKey());
                    preparedStatement.setString(4, record.getMessage());
                    preparedStatement.setLong(5, record.getDelayTime());
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch (Exception e) {
                log.error("batchStore[本地数据库异常] || records : {} ", records, e);
                ExitUtils.exit();
            }
        }
    }

    /**
     * 批量删除本地延时消息
     *
     * @param idList 延时消息id
     */
    public void batchDelete(List<Long> idList) {
        synchronized (writeLock) {
            try (
                    Connection connection = dataSource.getConnection();
                    PreparedStatement preparedStatement = connection.prepareStatement(SQL_DELETE)
            ) {
                connection.setAutoCommit(false);
                for (Long aLong : idList) {
                    preparedStatement.setLong(1, aLong);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch (Exception e) {
                log.error("batchDelete[本地数据库异常] || idList : {} ", idList, e);
                ExitUtils.exit();
            }
        }
    }

}
