using Delta.Connect;
using Google.Protobuf.WellKnownTypes;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.DeltaLake;

public class DeltaMergeBuilder
{
    private DeltaTable _targetTable;
    private DataFrame _source;
    private Column _onCondition;
    private List<MergeIntoTable.Types.Action> _whenMatchedClauses;
    private List<MergeIntoTable.Types.Action> _whenNotMatchedClauses;
    private List<MergeIntoTable.Types.Action> _whenNotMatchedBySourceClauses;
    private bool _schemaEvolution;
    

    public DeltaMergeBuilder(DeltaTable target, DataFrame source, Column onCondition, bool schemaEvolution = true )
    {
        _targetTable = target;
        _source = source;
        _onCondition = onCondition;
        _schemaEvolution = schemaEvolution;

        _whenMatchedClauses = new List<MergeIntoTable.Types.Action>();
        _whenNotMatchedClauses = new List<MergeIntoTable.Types.Action>();
        _whenNotMatchedBySourceClauses = new List<MergeIntoTable.Types.Action>();
    }

    /// <summary>
    /// Update any target rows that match with these set clauses
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdate(IDictionary<Column, Column> setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Update any target rows that match with these set clauses
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdate(IDictionary<string, Column> setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Update any target rows that match with these set clauses
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdate(params (string key, Column value)[] setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Update any target rows that match with these set clauses
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdate(params (Column col, Column value)[] setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }


    /// <summary>
    /// Update any target rows that match the condition with these set clauses
    /// </summary>
    /// <param name="condition">Filter to match</param>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdate(string condition, IDictionary<Column, Column> setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Update any target rows that match the condition with these set clauses
    /// </summary>
    /// <param name="condition">Filter to match</param>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdate(string condition, IDictionary<string, Column> setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Update any target rows that match the condition with these set clauses
    /// </summary>
    /// <param name="condition">Filter to match</param>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdate(string condition, params (string key, Column value)[] setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Update any target rows that match the condition with these set clauses
    /// </summary>
    /// <param name="condition">Filter to match</param>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdate(string condition, params (Column col, Column value)[] setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Update any target rows that match the condition with these set clauses
    /// </summary>
    /// <param name="condition">Filter to match</param>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdate(Column condition, IDictionary<string, Column> setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = condition.Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Update any target rows that match the condition with these set clauses
    /// </summary>
    /// <param name="condition">Filter to match</param>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdate(Column condition, params (string key, Column value)[] setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = condition.Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Update any target rows that match the condition with these set clauses
    /// </summary>
    /// <param name="condition">Filter to match</param>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdate(Column condition, params (Column col, Column value)[] setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = condition.Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Update any target rows that match the condition with all the columns in the source
    /// </summary>
    /// <param name="condition">Filter to match</param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdateAll(string condition)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            UpdateStarAction = new MergeIntoTable.Types.Action.Types.UpdateStarAction()
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Update any target rows that match the condition with all the columns in the source
    /// </summary>
    /// <param name="condition">Filter to match</param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdateAll(Column condition)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = condition.Expression,
            UpdateStarAction = new MergeIntoTable.Types.Action.Types.UpdateStarAction()
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Update any target rows that match with all the columns in the source
    /// </summary>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedUpdateAll()
    {
        var action = new MergeIntoTable.Types.Action()
        {
            UpdateStarAction = new MergeIntoTable.Types.Action.Types.UpdateStarAction()
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Update any target rows that do not match the source with the setClauses columns
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceUpdate(IDictionary<Column, Column> setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Update any target rows that do not match the source with the setClauses columns
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceUpdate(IDictionary<string, Column> setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Update any target rows that do not match the source with the setClauses columns
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceUpdate(params (string key, Column value)[] setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Update any target rows that do not match the source with the setClauses columns
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceUpdate(params (Column col, Column value)[] setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Update any target rows that do not match the source with the setClauses columns
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceUpdate(string condition, IDictionary<Column, Column> setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Update any target rows that do not match the source with the setClauses columns
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceUpdate(string condition, IDictionary<string, Column> setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Update any target rows that do not match the source with the setClauses columns
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceUpdate(string condition, params (string key, Column value)[] setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Update any target rows that do not match the source with the setClauses columns
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceUpdate(string condition, params (Column col, Column value)[] setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Update any target rows that do not match the source with the setClauses columns
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceUpdate(Column condition, IDictionary<string, Column> setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = condition.Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Update any target rows that do not match the source with the setClauses columns
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceUpdate(Column condition, params (string key, Column value)[] setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = condition.Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Update any target rows that do not match the source with the setClauses columns
    /// </summary>
    /// <param name="setClauses"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceUpdate(Column condition, params (Column col, Column value)[] setClauses)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = condition.Expression,
            UpdateAction = new MergeIntoTable.Types.Action.Types.UpdateAction()
            {
                Assignments = { ToAssignments(setClauses) }
            }
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Insert any new rows that don't exist in target using the condition and the specified values 
    /// </summary>
    /// <param name="condition"></param>
    /// <param name="values"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedInsert(Column condition, params (Column col, Column value)[] values)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = condition.Expression,
            InsertAction = new MergeIntoTable.Types.Action.Types.InsertAction()
            {
                Assignments = { ToAssignments(values) }
            }
        };
        
        _whenNotMatchedClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Insert any new rows that don't exist in target using the condition and the specified values 
    /// </summary>
    /// <param name="condition"></param>
    /// <param name="values"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedInsert(string condition, params (Column col, Column value)[] values)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            InsertAction = new MergeIntoTable.Types.Action.Types.InsertAction()
            {
                Assignments = { ToAssignments(values) }
            }
        };
        
        _whenNotMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Insert any new rows that don't exist in target using the condition and the specified values 
    /// </summary>
    /// <param name="condition"></param>
    /// <param name="values"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedInsert(Column condition, IDictionary<string, Column> values)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = condition.Expression,
            InsertAction = new MergeIntoTable.Types.Action.Types.InsertAction()
            {
                Assignments = { ToAssignments(values) }
            }
        };
        
        _whenNotMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Insert any new rows that don't exist in target using the condition and the specified values 
    /// </summary>
    /// <param name="condition"></param>
    /// <param name="values"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedInsert(Column condition, IDictionary<Column, Column> values)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = condition.Expression,
            InsertAction = new MergeIntoTable.Types.Action.Types.InsertAction()
            {
                Assignments = { ToAssignments(values) }
            }
        };
        
        _whenNotMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Insert any new rows that don't exist in target using the condition and the specified values 
    /// </summary>
    /// <param name="condition"></param>
    /// <param name="values"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedInsert(string condition, IDictionary<Column, Column> values)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            InsertAction = new MergeIntoTable.Types.Action.Types.InsertAction()
            {
                Assignments = { ToAssignments(values) }
            }
        };
        
        _whenNotMatchedClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Insert any new rows that don't exist in target using the condition and the specified values 
    /// </summary>
    /// <param name="condition"></param>
    /// <param name="values"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedInsert(string condition, IDictionary<string, Column> values)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            InsertAction = new MergeIntoTable.Types.Action.Types.InsertAction()
            {
                Assignments = { ToAssignments(values) }
            }
        };
        
        _whenNotMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Insert any new rows that don't exist in target using the condition and the specified values 
    /// </summary>
    /// <param name="condition"></param>
    /// <param name="values"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedInsert(params (Column col, Column value)[] values)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            InsertAction = new MergeIntoTable.Types.Action.Types.InsertAction()
            {
                Assignments = { ToAssignments(values) }
            }
        };
        
        _whenNotMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Insert any new rows that don't exist in target using the condition and the specified values 
    /// </summary>
    /// <param name="condition"></param>
    /// <param name="values"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedInsert(IDictionary<string, Column> values)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            InsertAction = new MergeIntoTable.Types.Action.Types.InsertAction()
            {
                Assignments = { ToAssignments(values) }
            }
        };
        
        _whenNotMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Insert any new rows that don't exist in target using the condition and the specified values 
    /// </summary>
    /// <param name="condition"></param>
    /// <param name="values"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedInsert(IDictionary<Column, Column> values)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            InsertAction = new MergeIntoTable.Types.Action.Types.InsertAction()
            {
                Assignments = { ToAssignments(values) }
            }
        };
        
        _whenNotMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Insert any new rows that don't exist in target using the condition with all the columns from the soure
    /// </summary>
    /// <param name="condition"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedInsertAll(string condition)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            InsertStarAction = new MergeIntoTable.Types.Action.Types.InsertStarAction()
        };
        
        _whenNotMatchedClauses.Add(action);
        return this;
    }

    /// <summary>
    /// Insert any new rows that don't exist in target using the condition with all the columns from the soure
    /// </summary>
    /// <param name="condition"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedInsertAll(Column condition)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = condition.Expression,
            InsertStarAction = new MergeIntoTable.Types.Action.Types.InsertStarAction()
        };
        
        _whenNotMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Insert any new rows that don't exist in target with all the columns from the soure
    /// </summary>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedInsertAll()
    {
        var action = new MergeIntoTable.Types.Action()
        {
            InsertStarAction = new MergeIntoTable.Types.Action.Types.InsertStarAction()
        };
        
        _whenNotMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Delete any rows that are in the source and match condition
    /// </summary>
    /// <param name="condition"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedDelete(string condition)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
           DeleteAction = new MergeIntoTable.Types.Action.Types.DeleteAction()
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Delete any rows that are in the source and match condition
    /// </summary>
    /// <param name="condition"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedDelete(Column condition)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = condition.Expression,
            DeleteAction = new MergeIntoTable.Types.Action.Types.DeleteAction()
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Delete any rows that are in the source
    /// </summary>
    /// <returns></returns>
    public DeltaMergeBuilder WhenMatchedDelete()
    {
        var action = new MergeIntoTable.Types.Action()
        {
            DeleteAction = new MergeIntoTable.Types.Action.Types.DeleteAction()
        };
        
        _whenMatchedClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Delete any rows that are in the source and not the target and also the target matches condition
    /// </summary>
    /// <param name="condition"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceDelete(string condition)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = Functions.Expr(condition).Expression,
            DeleteAction = new MergeIntoTable.Types.Action.Types.DeleteAction()
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Delete any rows that are in the source and not the target and also the target matches condition
    /// </summary>
    /// <param name="condition"></param>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceDelete(Column condition)
    {
        var action = new MergeIntoTable.Types.Action()
        {
            Condition = condition.Expression,
            DeleteAction = new MergeIntoTable.Types.Action.Types.DeleteAction()
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }
    
    /// <summary>
    /// Delete any rows that are in the source and not the target
    /// </summary>
    /// <returns></returns>
    public DeltaMergeBuilder WhenNotMatchedBySourceDelete()
    {
        var action = new MergeIntoTable.Types.Action()
        {
            DeleteAction = new MergeIntoTable.Types.Action.Types.DeleteAction()
        };
        
        _whenNotMatchedBySourceClauses.Add(action);
        return this;
    }
    
    private IEnumerable<Assignment> ToAssignments((string key, Column value)[] setClauses){
        return setClauses.Select(setClause => new Assignment() { Field = Functions.Expr(setClause.key).Expression, Value = setClause.value.Expression}).ToList();    
    }
    
    private IEnumerable<Assignment> ToAssignments((Column col, Column value)[] setClauses){
        return setClauses.Select(setClause => new Assignment() { Field  = setClause.col.Expression, Value = setClause.value.Expression}).ToList();    
    }
    
    
    private IEnumerable<Delta.Connect.Assignment> ToAssignments(IDictionary<string, Column> setClauses)
    {
        return setClauses.Select(setClause => new Delta.Connect.Assignment() { Field = Functions.Expr(setClause.Key).Expression, Value = setClause.Value.Expression}).ToList();
    }
    
    private IEnumerable<Delta.Connect.Assignment> ToAssignments(IDictionary<Column, Column> setClauses)
    {
        return setClauses.Select(setClause => new Delta.Connect.Assignment() { Field = setClause.Key.Expression, Value = setClause.Value.Expression}).ToList();
    }

    public DeltaMergeBuilder WithSchemaEvolution()
    {
        _schemaEvolution = true;
        return this;
    }
    
    /// <summary>
    /// Execute the merge request
    /// </summary>
    /// <param name="sparkSession"></param>
    public void Execute(SparkSession sparkSession)
    {
        var mergeIntoTable = new MergeIntoTable()
        {
            Target = _targetTable.ToDF().Relation,
            Source = _source.Relation,
            Condition = _onCondition.Expression,
            WithSchemaEvolution = _schemaEvolution
        };

        foreach (var action in _whenMatchedClauses)
        {
            mergeIntoTable.MatchedActions.Add(action);
        }
        
        foreach(var action in _whenNotMatchedClauses)
        {
            mergeIntoTable.NotMatchedActions.Add(action);
        }

        foreach(var action in _whenNotMatchedBySourceClauses)
        {
            mergeIntoTable.NotMatchedBySourceActions.Add(action);
        }

        mergeIntoTable.WithSchemaEvolution = _schemaEvolution;
        
        var relation = new DeltaRelation()
        {
            MergeIntoTable = mergeIntoTable
        };

        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };
        
        var df = new DataFrame(sparkSession, sparkRelation);
        df.Collect();
    }
}
//
// public class DeltaMergeMatchedActionBuilder(DeltaMergeBuilder builder, Column? matchCondition)
// {
//     public DeltaMergeBuilder Update(IDictionary<string, Column> setClauses)
//     {
//         MergeAction a = new MergeAction()
//         {
//             ActionType = MergeAction.Types.ActionType.Update,
//             Assignments = {  }
//         }
//     }
// }