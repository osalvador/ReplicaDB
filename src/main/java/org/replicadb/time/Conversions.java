package org.replicadb.time;

import java.sql.Date;
import java.time.*;

/**
 * Date and Time Epoch conversions utilities
 */
public final class Conversions {

    /**
     * Obtains an instance of LocalDate from the number of days since the epoch.
     * This returns a LocalDate with the specified epoch-of-day.
     * @param epochDay the number of days from epoch
     * @return the local date, not null
     */
    public static LocalDate dateOfEpochDay(long epochDay){
        return LocalDate.ofEpochDay(epochDay);
    }

    /**
     * Obtains an instance of sql.Date from the number of days since the epoch.
     * This returns a Date with the specified epoch-of-day.
     * @param epochDay the number of days from epoch
     * @return the date, not null
     */
    public static Date sqlDateOfEpochDay(long epochDay){
        return Date.valueOf(dateOfEpochDay(epochDay));
    }

    /**
     * Obtains an instance of OffsetDateTime using milliseconds from the epoch
     * @param epochMilli the number of milliseconds from epoch
     * @return the offset date-time, not null
     */
    public static OffsetDateTime timestampOfEpochMilli(long epochMilli) {
        Instant instant = Instant.ofEpochMilli(epochMilli);
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    /**
     * Obtains an instance of OffsetDateTime using microseconds from the epoch
     * @param epochMicro the number of microseconds from epoch
     * @return the offset date-time, not null
     */
    public static OffsetDateTime timestampOfEpochMicro(long epochMicro) {
        long epochSeconds = epochMicro / 1_000_000L;
        long nanoOffset = ( epochMicro % 1_000_000L ) * 1_000L ;
        Instant instantMicroTimestamp = Instant.ofEpochSecond( epochSeconds, nanoOffset );
        return OffsetDateTime.ofInstant(instantMicroTimestamp, ZoneOffset.UTC);
    }

    /**
     * Obtains an instance of OffsetDateTime using nanoseconds from the epoch
     * @param epochNano the number of nanoseconds from epoch
     * @return the offset date-time, not null
     */
    public static OffsetDateTime timestampOfEpochNano(long epochNano) {
        Instant instantNanoTimestamp = Instant.ofEpochSecond( 0, epochNano );
        return OffsetDateTime.ofInstant(instantNanoTimestamp, ZoneOffset.UTC);
    }

    /**
     * Obtains an instance of LocalTime from the number of nanoseconds past midnight.
     * This returns a LocalTime with the specified nanosecond-of-day.
     * @param nanoOfDay the nano of day,
     * @return the local time, not null
     */
    public static LocalTime timeOfNanoOfDay(long nanoOfDay){
        return LocalTime.ofNanoOfDay(nanoOfDay);
    }

    /**
     * Obtains an instance of LocalTime from the number of microseconds past midnight.
     * This returns a LocalTime with the specified microsecond-of-day.
     * @param microOfDay the nano of day,
     * @return the local time, not null
     */
    public static LocalTime timeOfMicroOfDay(long microOfDay){
        return LocalTime.ofNanoOfDay(microOfDay*1000L);
    }

    /**
     * Obtains an instance of LocalTime from the number of milliseconds past midnight.
     * This returns a LocalTime with the specified millisecond-of-day.
     * @param milliOfDay the nano of day,
     * @return the local time, not null
     */
    public static LocalTime timeOfMilliOfDay(long milliOfDay){
        return LocalTime.ofNanoOfDay(milliOfDay*1000000L);
    }



}
