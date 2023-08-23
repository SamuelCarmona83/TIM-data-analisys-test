SELECT
    ROW_NUMBER() OVER (ORDER BY CLAP.FECHA_TRANSACCION) AS conciliation_id,
    CLAP.INICIO6_TARJETA AS clap_inicio6_tarjeta,
    CLAP.FINAL4_TARJETA AS clap_final4_tarjeta,
    BANSUR.TARJETA AS bansur_tarjeta,
    CLAP.TIPO_TRX AS clap_tipo_trx,
    BANSUR.TIPO_TRX AS bansur_tipo_trx,
    CLAP.MONTO AS clap_monto,
    BANSUR.MONTO AS bansur_monto,
    CLAP.FECHA_TRANSACCION AS clap_fecha_transaccion,
    BANSUR.FECHA_TRANSACCION AS bansur_fecha_transaccion,
    CLAP.CODIGO_AUTORIZACION AS clap_codigo_autorizacion,
    BANSUR.CODIGO_AUTORIZACION AS bansur_codigo_autorizacion,
    CASE
        WHEN CLAP.INICIO6_TARJETA = LEFT(BANSUR.TARJETA, 6)
             AND CLAP.FINAL4_TARJETA = RIGHT(BANSUR.TARJETA, 4)
             AND ABS(CLAP.MONTO - BANSUR.MONTO) <= 0.99
             AND CLAP.FECHA_TRANSACCION = BANSUR.FECHA_TRANSACCION THEN 'Cruzada'
        ELSE 'No Cruzada'
    END AS cruzada
FROM
    clap AS CLAP
LEFT JOIN
    bansur AS BANSUR ON CLAP.INICIO6_TARJETA = LEFT(BANSUR.TARJETA, 6)
                              AND CLAP.FINAL4_TARJETA = RIGHT(BANSUR.TARJETA, 4)
                              AND ABS(CLAP.MONTO - BANSUR.MONTO) <= 0.99
                              AND CLAP.FECHA_TRANSACCION = BANSUR.FECHA_TRANSACCION
ORDER BY
    CLAP.FECHA_TRANSACCION;