select 05                     as codigoArquivo,
       'Dedução dos Créditos' as arquivo,
       'DEBITO'               as tipoArquivo,
       coalesce(arquivo.valor,0)          as valorArquivo,
       coalesce(integracao.valor,0)       as valorIntegracao
from (
    select sum(filtro.vlDeducao) as valor
      from (

        select coalesce(tdc.vlDeducao, 0) as vlDeducao
            from tceDeducaoCredito tdc
                     join tceDeducaoPagamento tdpag on tdpag.idtcededucaocredito = tdc.idtcededucaocredito
                     join tribpagamentodebito tpd on tpd.iddebitoparcelareceita = tdpag.iddebitoparcelareceita
                                                    and tpd.idpagamentobloqueto = tdpag.idpagamentobloqueto
                     join tribpagamentobloqueto tpb on tpb.idpagamentobloqueto = tpd.idpagamentobloqueto
            where tdc.dtdeducao between :dataInicial and :dataFinal
              and tpb.entidade = :entidade

        union all

        select coalesce(tdc.vlDeducao, 0) as vlDeducao
            from tceDeducaoCredito tdc
                     join tceDeducaoCancelamento tdcan on tdcan.idtcededucaocredito = tdc.idtcededucaocredito
                     join tribcancelamentodebitoitem tcdi on tcdi.entidade = tdcan.entidade
                                                            and tcdi.exercicio = tdcan.exercicio
                                                            and tcdi.idcancelamento = tdcan.idcancelamento
                                                            and tcdi.iddebitoparcelareceita = tdcan.iddebitoparcelareceita
            where tdc.dtdeducao between :dataInicial and :dataFinal
              and tcdi.entidade = :entidade

        union all

        select coalesce(tdc.vlDeducao, 0) as vlDeducao
            from tceDeducaoCredito tdc
                     join tceDeducaoIsencao tdise on tdise.idtcededucaocredito = tdc.idtcededucaocredito
                     join tribdebitoreceita tdr on tdr.iddebitoreceita = tdise.iddebitoreceita
            where tdc.dtdeducao between :dataInicial and :dataFinal
              and tdr.entidade = :entidade

        union all

        select coalesce(tdc.vlDeducao, 0) as vlDeducao
            from tceDeducaoCredito tdc
                     join tceDeducaoCreditoCredito tcred on tcred.idTceDeducaoCredito = tdc.idTceDeducaoCredito
                     join tribcreditocontribuinteitem tcci on tcci.idcreditocontribuinteitem = tcred.idcreditocontribuinteitem
                                                            and tcci.iddebitoparcelareceita = tcred.iddebitoparcelareceita
            where tdc.dtdeducao between :dataInicial and :dataFinal
              and tcci.entidade = :entidade

        ) filtro ) as arquivo
         cross join (select sum(valor) as valor
                     from (select iip.valor as valor
                           from integracaopatrimonial ip
                                    left join itemintegracaopatrimonial iip on ip.id = iip.idintegracaopatrimonial
                                    left join tribreceita r on r.receita = iip.tributo
                                                              and r.exercicio = ip.exercicio
                                                              and r.entidade = ip.entidade
                                    JOIN tribreceitatipo trt ON trt.tiporeceita = r.tiporeceita
                           where ip.entidade = :entidade
                             and trt.classificacaoreceitatipo IN (1, 2, 3, 4, 5, 7)
                             and ip.datamovimento between :dataInicial and :dataFinal
                             and iip.tipomovimento in ('CANCELAMENTO', 'DACAO_PAGAMENTO')
                             and iip.situacaolegal = 0
                             and iip.tipodeducao in (1, 2, 3, 4, 5, 6, 7)
                             and ip.situacaointegracaopatrimonial != 'ESTORNADO_CONTABILIDADE'
                           union all
                           select liic.valordeducao
                           from loteintegracaocontabil lic
                                    left join loteitemintegracaocontabil liic on lic.id = liic.idlote
                                    left join tribreceita tr on liic.idtributo = tr.id
                                    left JOIN tribreceitatipo trt ON trt.tiporeceita = tr.tiporeceita
                                    left join tribmotivodesconto tmd on liic.idmotivodeducao = tmd.motivodesconto
                                    left join tribtipodeducao ttd on tmd.tipodeducao = ttd.tipodeducao
                           where lic.datalote between :dataInicial and :dataFinal
                             and liic.situacaolegal = 0
                             and trt.classificacaoreceitatipo IN (1, 2, 3, 4, 5, 7)
                             and ttd.tipodeducao in (1, 2, 3, 4, 5, 6, 7)
                             and lic.situacaointegracaocontabil != 'ESTORNADO_CONTABILIDADE'
                             and lic.tipolote = 'PAGAMENTO'
                             and liic.valordeducao > 0) as dados) as integracao