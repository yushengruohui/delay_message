package com.yhh.dao;

import com.yhh.constant.DelayConst;
import com.yhh.dto.DelayDto;
import com.yhh.utils.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.JDBC;
import org.sqlite.SQLiteDataSource;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
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
    private static final String SQL_CREATE_TABLE = "create table if not exists common_delay_msg(id integer primary key not null ,topic text not null,message_key text,message text not null,delay_epoch integer not null)";
    /**
     * 建立索引 sql 语句
     */
    private static final String SQL_INDEX = "create index idx_delay_epoch on common_delay_msg(delay_epoch);";
    /**
     * 保存本地延时消息 sql 语句
     */
    private static final String SQL_ADD = "insert into common_delay_msg (id,topic,message_key,message,delay_epoch) values (?,?,?,?,?);";
    /**
     * 删除本地延时消息 sql 语句
     */
    private static final String SQL_DELETE = "delete from common_delay_msg where id = ? ;";
    /**
     * 扫描本地延时消息sql
     */
    private static final String SQL_SCAN = "select id,topic,message_key,message from common_delay_msg where delay_epoch <= ? limit 10000";
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
            SystemUtils.exit();
        }
    }

    public DelayDao(int dbId) {
        initSQLiteDataSource(dbId);
    }

    /**
     * 扫描到达延期时限的消息
     *
     * @return
     */
    public List<DelayDto> scanTodoMsg() {
        // long start = System.currentTimeMillis();
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(SQL_SCAN)
        ) {
            int now = (int) (System.currentTimeMillis() / 1000L);
            statement.setInt(1, now);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<DelayDto> records = new ArrayList<>();
                while (resultSet.next()) {
                    DelayDto dto = new DelayDto();
                    dto.setId(resultSet.getLong(1));
                    dto.setTopic(resultSet.getString(2));
                    dto.setMessageKey(resultSet.getString(3));
                    dto.setMessage(resultSet.getString(4));
                    records.add(dto);
                }
                return records;
            }
        } catch (Exception e) {
            log.error("scanTodoMsg[本地数据库异常]", e);
            SystemUtils.exit();
        }
        // log.info("scanTodoMsg || 耗时: {} ms ", System.currentTimeMillis() - start);
        return Collections.emptyList();
    }

    /**
     * 批量存储延时消息到本地数据库
     *
     * @param qoList
     */
    public void batchStore(List<DelayDto> qoList) {
        // long start = System.currentTimeMillis();
        synchronized (writeLock) {
            try (
                    Connection connection = dataSource.getConnection();
                    PreparedStatement preparedStatement = connection.prepareStatement(SQL_ADD)
            ) {
                connection.setAutoCommit(false);
                for (DelayDto qo : qoList) {
                    preparedStatement.setLong(1, qo.getId());
                    preparedStatement.setString(2, qo.getTopic());
                    preparedStatement.setString(3, qo.getMessageKey());
                    preparedStatement.setString(4, qo.getMessage());
                    preparedStatement.setInt(5, qo.getDelayTime());
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch (Exception e) {
                log.error("batchStore[本地数据库异常] || records : {} ", qoList, e);
                SystemUtils.exit();
            }
        }
        // log.info("方法 : batchStore || 耗时: {} ms ", System.currentTimeMillis() - start);
    }

    /**
     * 批量删除本地延时消息
     *
     * @param idList 延时消息id
     */
    public void batchDelete(List<Long> idList) {
        // long start = System.currentTimeMillis();
        synchronized (writeLock) {
            try (
                    Connection connection = dataSource.getConnection();
                    PreparedStatement preparedStatement = connection.prepareStatement(SQL_DELETE)
            ) {
                connection.setAutoCommit(false);
                for (Long id : idList) {
                    preparedStatement.setLong(1, id);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch (Exception e) {
                log.error("batchDelete[本地数据库异常] || idList : {} ", idList, e);
                SystemUtils.exit();
            }
        }
        // log.info("方法 : batchDelete || 耗时: {} ms ", System.currentTimeMillis() - start);
    }

}
