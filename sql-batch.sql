select
coalesce(sum( distinct filtro.valor),0) as valor
from
(
   select distinct
          td.tipotce as idTipoDeducaoCredito,
          cd.datacancelamento as dtDeducao,
          coalesce(tcdi.valorAtualizado, tcdi.valor) as valor,
          tlc.nrCredito,
          tlc.nrAnoCredito,
          ca.idtce as cdControleLeiAto,
          substr(md.descricao || '. ' || coalesce(cd.motivo, ''), 0, 250) as dsMotivo,
          tcdi.entidade,
          tcdi.exercicio,
          tcdi.idcancelamento,
          cast(null as integer) as idpagamentobloqueto,
          cast(null as integer) as iddebitoreceita,
          tcdi.iddebitoparcelareceita,
          cast(null as integer) as idcreditocontribuinteitem
   from tribcancelamentodebito          cd
        join tribcancelamentodebitoitem tcdi on tcdi.entidade = cd.entidade and tcdi.exercicio = cd.exercicio and tcdi.idcancelamento = cd.idcancelamento
   left join tribmotivodesconto         md   on md.motivodesconto = cd.motivocancelamento
   left join tribtipodeducao            td   on td.tipodeducao = coalesce(md.tipodeducao, cd.tipodeducao)
        join tribdebitoparcelareceita   tdpr on tdpr.iddebitoparcelareceita = tcdi.iddebitoparcelareceita
        join tribreceita                tr   on tr.entidade = tdpr.entidade and tr.exercicio = tdpr.exercicio and tr.receita = tdpr.receita
        join tribreceitatipo            trt  on trt.tiporeceita = tr.tiporeceita
        join tribclassificacaoreceitatipo tcrt on tcrt.classificacaoreceitatipo = trt.classificacaoreceitatipo
   left join tcedebitolanccredito       tdlc on tdlc.iddebitoparcelareceita = tcdi.iddebitoparcelareceita
   left join tcelanccredito             tlc  on tlc.idtcelanccredito = tdlc.idtcelanccredito
   left join cgato                      ca   on ca.entidade = md.entidade and ca.idato = md.idato
     where (cd.datacancelamento between :dataInicial and :dataFinal) and cd.entidade = :entidade
     and td.tipotce in (1,2,3,4,5,6)
     and tcdi.situacaolegal = 0
     and tcrt.classificacaoreceitatipo IN (1, 2, 3, 4, 5, 7)
   union all
   select distinct
          ttd.tipotce as idTipoDeducaoCredito,
          tp.datalancamento as dtDeducao,
          coalesce(tpd.valordesconto,0) as valor,
          tlc.nrCredito,
          tlc.nrAnoCredito,
          ca.idtce as cdControleLeiAto,
          substr(tmd.descricao  || '. ' || coalesce(tbad.observacao, ''), 0, 250) as dsMotivo,
          tp.entidade,
          tp.exerciciopagamento as exercicio,
          cast(null as integer) as idcancelamento,
          tpd.idpagamentobloqueto,
          cast(null as integer) as iddebitoreceita,
          tpd.iddebitoparcelareceita,
          cast(null as integer) as idcreditocontribuinteitem
   from tribpagamentodebito tpd
        join tribdebitoparcelareceita   tdpr on tdpr.iddebitoparcelareceita  = tpd.iddebitoparcelareceita
        join tribdebitoparcela          tdp  on tdp.entidade = tdpr.entidade and tdp.exercicio  = tdpr.exercicio and tdp.tipocadastro = tdpr.tipocadastro
                                            and tdp.cadastrogeral = tdpr.cadastrogeral and tdp.guiarecolhimento = tdpr.guiarecolhimento
                                            and tdp.subdivida = tdpr.subdivida and tdp.parcela = tdpr.parcela
        join tribpagamentobloqueto      tpb  on tpb.idpagamentobloqueto  = tpd.idpagamentobloqueto
        join tribpagamento              tp   on tp.entidade = tpb.entidade and tp.exerciciopagamento = tpb.exerciciopagamento and tp.pagamento = tpb.pagamento
   left join tribbaixaautomaticadetalhe tbad on tbad.entidade = tp.entidade and tbad.exerciciopagamento = tp.exerciciopagamento and tbad.pagamento = tp.pagamento
   left join tribmotivodesconto         tmd  on tmd.motivodesconto = tpb.motivodesconto
   left join tribtipodeducao            ttd  on ttd.tipodeducao = tmd.tipodeducao
        join tribreceita                tr   on tr.entidade = tdpr.entidade and tr.exercicio = tdpr.exercicio and tr.receita = tdpr.receita
        join tribreceitatipo            trt  on trt.tiporeceita = tr.tiporeceita
        join tribclassificacaoreceitatipo tcrt on tcrt.classificacaoreceitatipo = trt.classificacaoreceitatipo
   left join tcedebitolanccredito       tdlc on tdlc.iddebitoparcelareceita = tdpr.iddebitoparcelareceita
   left join tcelanccredito             tlc  on tlc.idtcelanccredito = tdlc.idtcelanccredito
   left join cgato                      ca   on ca.entidade = tmd.entidade and ca.idato = tmd.idato
   where tp.datalancamento between :dataInicial and :dataFinal and tp.entidade = :entidade
   and tpd.valordesconto > 0
   and tdp.situacaolegal = 0
   and ttd.tipotce in (1,2,3,4,5,6)
   and tcrt.classificacaoreceitatipo IN (1, 2, 3, 4, 5, 7)

   union all

   select distinct
          ttd.tipotce as idTipoDeducaoCredito,
          coalesce(td.datacontabilizacao, td.datainclusao) as dtDeducao,
          coalesce(tdri.valorisencao, 0) as valor,
          tlc.nrCredito,
          tlc.nrAnoCredito,
          ca.idtce as cdControleLeiAto,
          tmd.descricao as dsMotivo,
          td.entidade,
          td.exercicio,
          cast(null as integer) as idcancelamento,
          cast(null as integer) as idpagamentobloqueto,
          tdr.iddebitoreceita,
          cast(null as integer) as iddebitoparcelareceita,
          cast(null as integer) as idcreditocontribuinteitem
   from tribdebitoreceitaisencao          tdri
        join tribdebitoreceita            tdr  on tdr.entidade = tdri.entidade and tdr.exercicio = tdri.exercicio and tdr.tipocadastro = tdri.tipocadastro
                                              and tdr.cadastrogeral = tdri.cadastrogeral and tdr.guiarecolhimento = tdri.guiarecolhimento
                                              and tdr.subdivida = tdri.subdivida and tdr.receita = tdri.receita
        join tribdebito                   td   on td.entidade = tdri.entidade and td.exercicio = tdri.exercicio and td.tipocadastro = tdri.tipocadastro
                                              and td.cadastrogeral = tdri.cadastrogeral and td.guiarecolhimento = tdri.guiarecolhimento and td.subdivida = tdri.subdivida
        join tribisencao                  i    on i.isencao = tdri.isencao
        join tribreceita                  tr   on tr.exercicio = tdr.exercicio and tr.receita = tdr.receita and tr.entidade = tdr.entidade
        join tribreceitatipo              trt  on trt.tiporeceita = tr.tiporeceita
        join tribclassificacaoreceitatipo tcrt on tcrt.classificacaoreceitatipo = trt.classificacaoreceitatipo
   left join tribmotivodesconto           tmd  on tmd.motivodesconto  = i.motivodesconto
   left join tribtipodeducao              ttd  on ttd.tipodeducao = i.tipodeducao
        join tribdebitoparcelareceita     tdpr on tdpr.entidade = tdri.entidade and tdpr.exercicio = tdri.exercicio and tdpr.tipocadastro = tdri.tipocadastro
                                              and tdpr.cadastrogeral = tdri.cadastrogeral and tdpr.guiarecolhimento = tdri.guiarecolhimento
                                              and tdpr.subdivida = tdri.subdivida and tdpr.receita = tdri.receita
   left join tcedebitolanccredito         tdlc on tdlc.iddebitoparcelareceita = tdpr.iddebitoparcelareceita
   left join tcelanccredito               tlc  on tlc.idtcelanccredito = tdlc.idtcelanccredito
   left join cgato                        ca   on ca.entidade = tmd.entidade and ca.idato = tmd.idato
   where (((td.DataInclusao between :dataInicial and :dataFinal) and td.DataContabilizacao is null) or
           (td.DataContabilizacao between :dataInicial and :dataFinal))
   and td.entidade = :entidade
   and tdri.valorisencao > 0
   and ttd.tipotce in (1,2,3,4,5,6)
   and tcrt.classificacaoreceitatipo in (1,2,3,4,5,7)
   and td.constituido = 'S'

   union all

   select distinct
          ttd.tipotce as idTipoDeducaoCredito,
          tcc.datacredito as dtDeducao,
          coalesce(tcci.valordesconto, 0) as valor,
          tlc.nrCredito,
          tlc.nrAnoCredito,
          ca.idtce as cdControleLeiAto,
          substr(tmd.descricao, 0, 250) as dsMotivo,
          tcc.entidade,
          tcc.exercicio,
          cast(null as integer) as idcancelamento,
          cast(null as integer) as idpagamentobloqueto,
          cast(null as integer) as iddebitoreceita,
          tcci.iddebitoparcelareceita,
          tcci.idcreditocontribuinteitem
   from tribbaixaautomaticadetalhe        tbad
        join tribcreditocontribuinte      tcc   on tcc.entidade = tbad.entidade and tcc.exercicio  = tbad.exerciciocredito and tcc.creditocontribuinte = tbad.creditocontribuinte
        join tribCreditoContribuinteItem  tcci  on tcci.entidade = tcc.entidade and tcci.exercicio = tcc.exercicio and tcci.creditocontribuinte = tcc.creditocontribuinte
        join tribdebitoparcelareceita     tdpr  on tdpr.iddebitoparcelareceita  = tcci.iddebitoparcelareceita
        join tribdebitoparcela            tdp   on tdp.entidade = tdpr.entidade and tdp.exercicio  = tdpr.exercicio and tdp.tipocadastro = tdpr.tipocadastro
                                               and tdp.cadastrogeral  = tdpr.cadastrogeral and tdp.guiarecolhimento = tdpr.guiarecolhimento
                                               and tdp.subdivida  = tdpr.subdivida and tdp.parcela = tdpr.parcela
        join tribbloqueto                 tb    on tb.entidade = tbad.entidade and tb.exerciciobloqueto = tbad.exerciciobloqueto and tb.bloqueto = tbad.bloqueto
        join tribGuiaRecolhimentoFormaPag tgrfp on tgrfp.entidade = tdpr.entidade and tgrfp.exercicio = tdpr.exercicio and tgrfp.guiarecolhimento = tdpr.guiarecolhimento and tgrfp.formapagamento = tb.formapagamento
        join tribreceita                  tr    on tr.exercicio = tdpr.exercicio and tr.receita = tdpr.receita and tr.entidade = tdpr.entidade
        join tribreceitatipo              trt   on trt.tiporeceita = tr.tiporeceita
        join tribclassificacaoreceitatipo tcrt  on tcrt.classificacaoreceitatipo = trt.classificacaoreceitatipo
   left join tribMotivoDesconto           tmd   on tmd.motivodesconto = coalesce(tgrfp.motivodesconto, tb.motivodesconto)
   left join tribTipoDeducao              ttd   on ttd.tipodeducao  = tmd.tipodeducao
   left join tcedebitolanccredito         tdlc  on tdlc.iddebitoparcelareceita = tdpr.iddebitoparcelareceita
   left join tcelanccredito               tlc   on tlc.idtcelanccredito = tdlc.idtcelanccredito
   left join cgato                        ca    on ca.entidade = tcci.entidade and ca.idato = tmd.idato
     WHERE tcc.entidade = :entidade AND tcc.datacredito BETWEEN :dataInicial and :dataFinal
     and tcci.valordesconto > 0
     and tdp.situacaolegal = 0
     and ttd.tipotce in (1,2,3,4,5,6)
     and tcrt.classificacaoreceitatipo in (1,2,3,4,5,7)
) filtro ;